"""Source generation and cloud upload are separate, explicit commands."""
import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys

from .generator import Config, generate
from .upload import execute_upload, load_storage_config, oci_client, upload_plan, verify_uploaded_objects
from .validation import validate_dataset


def main(argv=None):
    parser=argparse.ArgumentParser(prog="aidp")
    commands=parser.add_subparsers(dest="command",required=True)
    gen=commands.add_parser("generate",help="Create deterministic extracts outside the repository")
    gen.add_argument("--seed",type=int,default=42)
    gen.add_argument("--reference-date",type=date.fromisoformat,required=True)
    gen.add_argument("--historical-orders",type=int,default=30_000)
    gen.add_argument("--open-orders",type=int,default=600)
    gen.add_argument("--output-dir",type=Path,required=True)
    check=commands.add_parser("validate",help="Check relationships, outcomes, changes and injected defects")
    check.add_argument("--data-dir",type=Path,required=True)
    upload=commands.add_parser("upload",help="Upload one selected batch to an EXISTING OCI bucket")
    upload.add_argument("--data-dir",type=Path,required=True)
    upload.add_argument("--batch",choices=["initial","incremental"],required=True)
    upload.add_argument("--storage-config",type=Path,default=os.getenv("AIDP_STORAGE_CONFIG"),
                        help="Non-secret JSON destination configuration outside Git")
    upload.add_argument("--namespace")
    upload.add_argument("--bucket")
    upload.add_argument("--prefix")
    upload.add_argument("--region")
    upload.add_argument("--profile")
    upload.add_argument("--config-file")
    upload.add_argument("--auth",choices=["api_key","security_token"])
    upload.add_argument("--include-policies",action="store_true",help="Initial uploads always include policies")
    upload.add_argument("--dry-run",action="store_true",help="No SDK/auth/network use; unresolved config is shown as placeholders")
    upload.add_argument("--overwrite",action="store_true",help="Explicitly permit replacing existing objects")
    args=parser.parse_args(argv)
    try:
        if args.command=="generate":
            generate(Config(args.seed,args.reference_date,args.historical_orders,args.open_orders),args.output_dir)
            result=validate_dataset(args.output_dir)
            result["output_directory"]=str(args.output_dir.expanduser().resolve())
        elif args.command=="validate": result=validate_dataset(args.data_dir)
        else:
            settings=load_storage_config(Path(args.storage_config)) if args.storage_config else {}
            defaults={"profile":"DEFAULT", "config_file":"~/.oci/config", "auth":"api_key"}
            environment={"namespace":"AIDP_OCI_NAMESPACE", "bucket":"AIDP_OCI_BUCKET", "prefix":"AIDP_OCI_PREFIX",
                         "region":"OCI_CLI_REGION", "profile":"OCI_CLI_PROFILE", "config_file":"OCI_CLI_CONFIG_FILE",
                         "auth":"OCI_CLI_AUTH"}
            for field, variable in environment.items():
                if getattr(args,field) is None:
                    setattr(args,field,os.getenv(variable) or settings.get(field) or defaults.get(field))
            if args.auth not in ("api_key","security_token"):
                raise ValueError("Select supported --auth api_key or security_token")
            plan=upload_plan(args.data_dir.expanduser().resolve(),args.batch,args.prefix,
                             args.batch=="initial" or args.include_policies)
            configured=bool(args.namespace and args.bucket and args.prefix)
            if args.dry_run:
                result=dict(dry_run=True,storage_configured=configured,cloud_verified=False,
                            namespace=args.namespace or "<namespace>",bucket=args.bucket or "<existing-bucket>",
                            profile=args.profile,region=args.region or "<profile-region>",
                            overwrite=args.overwrite,objects=plan)
            else:
                if not configured: raise ValueError("Configure --namespace, --bucket and --prefix (or AIDP_OCI_*); no upload attempted")
                client=oci_client(args.config_file,args.profile,args.auth,args.region)
                result=dict(dry_run=False,results=execute_upload(client,args.namespace,args.bucket,plan,args.overwrite))
                result["verification"]=verify_uploaded_objects(client,args.namespace,args.bucket,plan)
        print(json.dumps(result,indent=2,sort_keys=True))
        return 0
    except (ValueError,RuntimeError,OSError,KeyError) as error:
        print(f"Error: {error}",file=sys.stderr)
        return 1
