import contextlib
import csv
from datetime import date
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from aidp_lakehouse.cli import main
from aidp_lakehouse.generator import Config, digest, generate, json_write
from aidp_lakehouse.upload import execute_upload, load_storage_config, upload_plan, verify_uploaded_objects
from aidp_lakehouse.validation import read_records, validate_dataset


class ServiceError(Exception):
    def __init__(self,status): self.status=status


class FakeClient:
    def __init__(self,existing=None,race=False):
        self.objects=existing or {}
        self.calls=[]
        self.race=race

    def head_bucket(self,*args): self.calls.append("head_bucket")

    def head_object(self,namespace,bucket,name):
        self.calls.append("head_object")
        if name not in self.objects: raise ServiceError(404)
        return SimpleNamespace(headers=self.objects[name])

    def put_object(self,namespace,bucket,name,stream,**kwargs):
        self.calls.append("put_object")
        if self.race: raise ServiceError(412)
        if kwargs.get("if_none_match")=="*" and name in self.objects: raise ServiceError(412)
        body=stream.read()
        self.objects[name]={"content-length":str(len(body)),"opc-meta-sha256":kwargs["opc_meta"]["sha256"]}
        self.last_kwargs=kwargs

    def list_objects(self,namespace,bucket,**kwargs):
        objects=[SimpleNamespace(name=name,size=int(headers['content-length']))
                 for name,headers in self.objects.items() if name.startswith(kwargs['prefix'])]
        return SimpleNamespace(data=SimpleNamespace(objects=objects,next_start_with=None))


class SourceDataTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="aidp-test-")
        self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)
        self.root=self.base/"data"
        self.config=Config(seed=42,reference_date=date(2026,10,28),historical_orders=1000,open_orders=30)
        generate(self.config,self.root)

    def rehash(self,batch,name):
        directory=self.root/batch
        m=json.loads((directory/"manifest.json").read_text())
        meta=json.loads((directory/"ingestion_metadata.json").read_text())
        m["sha256"][name]=meta["files"][name]["sha256"]=digest(directory/name)
        json_write(directory/"manifest.json",m)
        json_write(directory/"ingestion_metadata.json",meta)

    def edit_csv(self,batch,name,edit):
        path=self.root/batch/name
        rows=read_records(path)
        edit(rows)
        with path.open("w",newline="",encoding="utf-8") as stream:
            writer=csv.DictWriter(stream,fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
        self.rehash(batch,name)

    def test_reproducible_bytes_and_quality(self):
        other=self.base/"repeat"
        generate(self.config,other)
        left={str(p.relative_to(self.root)):p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        right={str(p.relative_to(other)):p.read_bytes() for p in other.rglob('*') if p.is_file()}
        self.assertEqual(left,right)
        report=validate_dataset(self.root)
        self.assertEqual(report["status"],"PASS")
        self.assertEqual(report["merged_orders"],1070)
        self.assertEqual(report["completed_orders"],1058)
        self.assertEqual(report["open_orders"],12)
        self.assertTrue(.05<report["late_rate"]<.65)
        self.assertEqual(report["incremental_exact_duplicates"],60)
        self.assertEqual(report["incremental_quarantined_orders"],15)

    def test_seed_and_reference_date_change_output(self):
        for seed,day in [(43,date(2026,10,28)),(42,date(2026,10,29))]:
            other=self.base/f"changed-{seed}-{day}"
            generate(Config(seed,day,1000,30),other)
            self.assertNotEqual(digest(self.root/"initial/orders.csv"),digest(other/"initial/orders.csv"))
            self.assertEqual(validate_dataset(other)["status"],"PASS")

    def test_rejects_overwrite_and_in_repository_generation(self):
        before=digest(self.root/"initial/orders.csv")
        with self.assertRaises(ValueError): generate(self.config,self.root)
        self.assertEqual(before,digest(self.root/"initial/orders.csv"))
        with self.assertRaises(ValueError): generate(self.config,Path(__file__).resolve().parents[1]/"generated")

    def test_rejects_leaking_updated_acceptance_feature(self):
        def edit(rows):
            target=next(r for r in rows if r["row_version"]=="2")
            target["units"]=str(int(target["units"])+1)
        self.edit_csv("incremental","orders.csv",edit)
        with self.assertRaisesRegex(ValueError,"Acceptance snapshot changed"): validate_dataset(self.root)

    def test_rejects_impossible_delivery_despite_matching_checksums(self):
        def edit(rows): rows[0]["delivered_at"]="2000-01-01T00:00:00Z"
        self.edit_csv("initial","deliveries.csv",edit)
        with self.assertRaisesRegex(ValueError,"chronology"): validate_dataset(self.root)

    def test_rejects_undocumented_defect(self):
        def edit(rows): rows[-1]["accepted_at"]="not-a-date"
        self.edit_csv("incremental","orders.csv",edit)
        with self.assertRaisesRegex(ValueError,"defects differ"): validate_dataset(self.root)

    def test_rejects_missing_valid_relationship(self):
        def edit(rows): rows[0]["order_id"]="O-NOT-PRESENT"
        self.edit_csv("initial","deliveries.csv",edit)
        with self.assertRaisesRegex(ValueError,"unknown order"): validate_dataset(self.root)

    def test_upload_batch_isolation_and_initial_documents(self):
        initial=upload_plan(self.root,"initial","fixture-prefix",True)
        incremental=upload_plan(self.root,"incremental","fixture-prefix",False)
        self.assertEqual(len(initial),8)
        self.assertEqual(len(incremental),6)
        self.assertFalse(any("incremental/" in p["object_name"] for p in initial))
        self.assertFalse(any("initial/" in p["object_name"] or "/documents/" in p["object_name"] for p in incremental))
        self.assertEqual({p["object_name"] for p in initial}, {
            "fixture-prefix/customers/initial/customers.csv",
            "fixture-prefix/orders/initial/orders.csv",
            "fixture-prefix/deliveries/initial/deliveries.csv",
            "fixture-prefix/events/initial/events.jsonl",
            "fixture-prefix/_control/initial/manifest.json",
            "fixture-prefix/_control/initial/ingestion_metadata.json",
            "fixture-prefix/documents/order_prioritization.md",
            "fixture-prefix/documents/incident_escalation.md"})
        control=[p for p in incremental if Path(p["local_path"]).name in ("manifest.json","ingestion_metadata.json")]
        self.assertTrue(all("/_control/incremental/" in p["object_name"] for p in control))
        self.assertFalse(any("manifest" in p["object_name"] for p in initial
                             if "/_control/" not in p["object_name"]))
        with self.assertRaises(ValueError): upload_plan(self.root,"initial","../unsafe",True)

    def test_dry_run_never_initializes_cloud_client(self):
        with patch("aidp_lakehouse.cli.oci_client",side_effect=AssertionError("Network must not run")):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                result=main(["upload","--data-dir",str(self.root),"--batch","initial","--dry-run"])
        self.assertEqual(result,0)
        report=json.loads(output.getvalue())
        self.assertTrue(report["dry_run"])
        self.assertFalse(report["cloud_verified"])
        self.assertEqual(len(report["objects"]),8)

    def test_upload_conditional_write_and_remote_checks(self):
        client=FakeClient()
        plan=upload_plan(self.root,"initial","fixture-prefix",True)
        result=execute_upload(client,"fixture-namespace","fixture-bucket",plan)
        self.assertEqual(len(result),8)
        self.assertEqual(client.calls[0],"head_bucket")
        self.assertEqual(client.last_kwargs["if_none_match"],"*")
        self.assertEqual(len(client.objects),8)
        verification=verify_uploaded_objects(client,"fixture-namespace","fixture-bucket",plan)
        self.assertEqual(verification['matched_count'],8)
        self.assertEqual(verification['total_objects_under_run_prefix'],8)
        client.objects.pop(plan[0]['object_name'])
        with self.assertRaisesRegex(ValueError,'names/sizes differ'):
            verify_uploaded_objects(client,"fixture-namespace","fixture-bucket",plan)

    def test_existing_object_refused_before_writes(self):
        plan=upload_plan(self.root,"initial","fixture-prefix",True)
        client=FakeClient({plan[-1]["object_name"]:{}})
        with self.assertRaisesRegex(ValueError,"already exists"):
            execute_upload(client,"fixture-namespace","fixture-bucket",plan)
        self.assertNotIn("put_object",client.calls)

    def test_concurrent_creation_fails_closed(self):
        plan=upload_plan(self.root,"initial","fixture-prefix",True)
        client=FakeClient(race=True)
        with self.assertRaisesRegex(RuntimeError,"412"):
            execute_upload(client,"fixture-namespace","fixture-bucket",plan)
        self.assertEqual(client.calls.count("put_object"),1)

    def test_missing_configuration_blocks_real_upload(self):
        with patch.dict("os.environ",{},clear=True),patch("aidp_lakehouse.cli.oci_client",side_effect=AssertionError("No cloud call")):
            with contextlib.redirect_stderr(io.StringIO()):
                result=main(["upload","--data-dir",str(self.root),"--batch","initial"])
        self.assertEqual(result,1)

    def test_external_configuration_and_cli_precedence(self):
        path=self.base/"storage.local.json"
        json_write(path,dict(namespace="fixture-namespace",bucket="fixture-bucket",prefix="fixture-prefix",
                             profile="fixture-profile",region="fixture-region"))
        self.assertEqual(load_storage_config(path)["bucket"],"fixture-bucket")
        with patch.dict("os.environ",{"AIDP_OCI_PREFIX":"environment-prefix"},clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                result=main(["upload","--data-dir",str(self.root),"--batch","initial","--dry-run",
                             "--storage-config",str(path),"--prefix","cli-prefix"])
        self.assertEqual(result,0)
        report=json.loads(output.getvalue())
        self.assertTrue(report["storage_configured"])
        self.assertFalse(report["cloud_verified"])
        self.assertEqual(report["profile"],"fixture-profile")
        self.assertEqual(report["region"],"fixture-region")
        self.assertTrue(all(r["object_name"].startswith("cli-prefix/") for r in report["objects"]))
        with patch.dict("os.environ",{"AIDP_OCI_PREFIX":"environment-prefix"},clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                main(["upload","--data-dir",str(self.root),"--batch","initial","--dry-run","--storage-config",str(path)])
        self.assertTrue(all(r["object_name"].startswith("environment-prefix/") for r in json.loads(output.getvalue())["objects"]))

    def test_configuration_rejects_credentials_and_git_file(self):
        path=self.base/"storage.local.json"
        json_write(path,dict(namespace="fixture",api_key="not-a-real-secret"))
        with self.assertRaisesRegex(ValueError,"unsupported fields"): load_storage_config(path)
        json_write(path,dict(namespace="<placeholder>"))
        with self.assertRaisesRegex(ValueError,"placeholders"): load_storage_config(path)
        tracked_example=Path(__file__).resolve().parents[1]/"config/storage.example.json"
        with self.assertRaisesRegex(ValueError,"outside Git"): load_storage_config(tracked_example)


if __name__=="__main__": unittest.main()
