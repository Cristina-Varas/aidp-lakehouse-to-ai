"""Explicit-file uploads to an existing OCI bucket; never creates resources."""
from __future__ import annotations

import mimetypes
import json
import posixpath
from pathlib import Path, PurePosixPath

from .generator import DATA_FILES, digest
from .validation import validate_dataset


STORAGE_FIELDS = {"namespace", "bucket", "prefix", "profile", "region", "auth", "config_file"}


def load_storage_config(path: Path) -> dict:
    """Read non-secret destination settings from an explicit file outside Git."""
    path = path.expanduser().resolve()
    if any((parent / ".git").exists() for parent in path.parents):
        raise ValueError("Local storage configuration must be outside Git")
    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict) or set(config) - STORAGE_FIELDS:
        raise ValueError("Storage configuration contains unsupported fields; credentials belong in external OCI config")
    if any(not isinstance(value, str) or not value.strip() or "<" in value or ">" in value for value in config.values()):
        raise ValueError("Replace storage placeholders with nonempty local values")
    if config.get("auth", "api_key") not in ("api_key", "security_token"):
        raise ValueError("Unsupported OCI authentication type")
    return config


def upload_plan(root: Path, batch: str, prefix: str | None, include_policies: bool) -> list[dict]:
    if batch not in ("initial", "incremental"):
        raise ValueError("Select initial or incremental explicitly")
    validate_dataset(root)
    prefix = prefix.strip("/") if prefix is not None else "<prefix>"
    if not prefix or any(p in (".", "..") for p in prefix.split("/")):
        raise ValueError("Use a nonempty object prefix without dot path components")
    selected = [(Path(batch)/name, Path(name.split(".")[0])/batch/name) for name in DATA_FILES]
    # Manifests describe intentionally injected defects. Never place them under
    # entity ingestion or document retrieval prefixes.
    selected.extend((Path(batch)/name, Path("_control")/batch/name)
                    for name in ("manifest.json", "ingestion_metadata.json"))
    if include_policies:
        selected.extend((Path("documents")/name, Path("documents")/name)
                        for name in ("order_prioritization.md", "incident_escalation.md"))
    plan=[]
    for relative, destination in selected:
        local=root/relative
        if local.is_symlink() or not local.is_file():
            raise ValueError("Upload selection must contain regular files")
        plan.append(dict(local_path=str(local.resolve()),
                         object_name=str(PurePosixPath(prefix)/destination.as_posix()),
                         bytes=local.stat().st_size, sha256=digest(local)))
    return plan


def execute_upload(client, namespace: str, bucket: str, plan: list[dict], overwrite=False) -> list[dict]:
    """Client injection supports offline contract tests without OCI credentials."""
    try:
        client.head_bucket(namespace, bucket)
        if not overwrite:
            # Refuse known collisions before any writes; conditional PUT also guards races.
            for item in plan:
                try: client.head_object(namespace,bucket,item["object_name"])
                except Exception as error:
                    if getattr(error,"status",None)!=404: raise
                else: raise ValueError("Object already exists; upload refused: "+item["object_name"])
        results=[]
        for item in plan:
            path=Path(item["local_path"])
            if digest(path)!=item["sha256"]: raise ValueError("Local file changed after planning")
            kwargs=dict(content_length=item["bytes"], opc_meta={"sha256":item["sha256"]},
                        content_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream")
            if not overwrite: kwargs["if_none_match"]="*"
            with path.open("rb") as stream:
                client.put_object(namespace,bucket,item["object_name"],stream,**kwargs)
            response=client.head_object(namespace,bucket,item["object_name"])
            if (int(response.headers.get("content-length",-1))!=item["bytes"] or
                response.headers.get("opc-meta-sha256")!=item["sha256"]):
                raise ValueError("Remote size/metadata verification failed: "+item["object_name"])
            results.append(dict(object_name=item["object_name"],status="UPLOADED_AND_HEAD_VERIFIED"))
        return results
    except ValueError: raise
    except Exception as error:
        # Do not print SDK request objects, headers, configuration, or credential material.
        raise RuntimeError(f"OCI upload/access failed (HTTP {getattr(error,'status','unavailable')}); no resource creation attempted") from None


def verify_uploaded_objects(client, namespace: str, bucket: str, plan: list[dict]) -> dict:
    """List only this demo/run prefix and match remote names and sizes to the plan."""
    if not plan:
        raise ValueError("Cannot verify an empty upload plan")
    prefix = posixpath.commonpath([str(PurePosixPath(p["object_name"]).parent) for p in plan]).rstrip("/") + "/"
    expected = {p["object_name"]: p["bytes"] for p in plan}
    remote = {}
    start = None
    try:
        while True:
            kwargs = {"prefix": prefix, "fields": "name,size"}
            if start:
                kwargs["start"] = start
            page = client.list_objects(namespace, bucket, **kwargs).data
            remote.update({obj.name: obj.size for obj in page.objects})
            start = page.next_start_with
            if not start:
                break
    except Exception as error:
        raise RuntimeError(f"Remote listing failed (HTTP {getattr(error, 'status', 'unavailable')})") from None
    if any(remote.get(name) != size for name, size in expected.items()):
        raise ValueError("Remote object names/sizes differ from the local upload plan")
    return dict(expected_count=len(expected), matched_count=len(expected),
                total_objects_under_run_prefix=len(remote), total_bytes=sum(expected.values()),
                additional_names=sorted(set(remote)-set(expected)),
                objects=[dict(object_name=name,bytes=remote[name]) for name in sorted(expected)])


def oci_client(config_file: str, profile: str, auth: str, region: str | None = None):
    try: import oci
    except ImportError:
        raise RuntimeError("Install the optional OCI SDK: python3 -m pip install -e '.[oci]'") from None
    try:
        config=oci.config.from_file(str(Path(config_file).expanduser()),profile)
        if region:
            config["region"] = region
        if auth=="security_token":
            token=Path(config["security_token_file"]).expanduser().read_text().strip()
            key=oci.signer.load_private_key_from_file(str(Path(config["key_file"]).expanduser()),config.get("pass_phrase"))
            signer=oci.auth.signers.SecurityTokenSigner(token,key)
            return oci.object_storage.ObjectStorageClient(config,signer=signer)
        return oci.object_storage.ObjectStorageClient(config)
    except Exception:
        raise RuntimeError("Unable to initialize OCI authentication; check the external config/profile without sharing credentials") from None
