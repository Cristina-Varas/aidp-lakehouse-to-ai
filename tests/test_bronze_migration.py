"""Metadata safety simulations; never connect to AIDP or OCI."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from aidp_lakehouse import bronze_migration as m
from aidp_lakehouse.aidp_bronze import BronzeConfig, columns


class Spark:
    def __init__(self):
        self.calls = []
    def sql(self, text):
        self.calls.append(text)
        return SimpleNamespace(collect=lambda: [(f'CREATE TABLE {text.split("TABLE ",1)[1]} (id STRING) USING CSV LOCATION \'old\'',)])


class MigrationTests(unittest.TestCase):
    def snapshots(self):
        return {name: dict(name=name, entity=entity,
                    detail={'type':'EXTERNAL','location': (m.TARGET if name==m.PROBE else BronzeConfig(prefix=m.V1)).location(entity)},
                    schema=[list(x) for x in columns(entity)], describe_rows=[['Type','EXTERNAL']],
                    ddl=f'CREATE EXTERNAL TABLE {name} (id STRING) USING CSV LOCATION \'captured-original\'')
                for name,entity in m.NAMES.items()}

    def prepare(self, directory, spark, snapshots):
        with patch.object(m,'_identity',return_value={'principal':'tester','application_id':'app'}), \
             patch.object(m,'_names',return_value={x.rsplit('.',1)[1] for x in snapshots}), \
             patch.object(m,'inspect_external',side_effect=lambda s,n:{k:v for k,v in snapshots[n].items() if k!='ddl'}), \
             patch.object(m,'_exact_ddl',side_effect=lambda s,n:snapshots[n]['ddl']), \
             patch.object(m,'_check_sources'), patch.object(m,'compare_v2',return_value={'rows':600}):
            return m.prepare(spark,directory)

    def test_prepare_is_read_only_and_captures_actual_definitions(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'backup';spark=Spark();snapshots=self.snapshots()
            result=self.prepare(directory,spark,snapshots)
            self.assertEqual(result['platform_changes'],0)
            self.assertEqual(spark.calls,[])
            self.assertIn('captured-original',(directory/'restore_definitions.sql').read_text())
            with patch.object(m,'_identity',return_value={'principal':'tester','application_id':'app'}):
                _,plan=m._load(spark,directory)
            with patch.object(m,'inspect_external',return_value={k:v for k,v in snapshots[m.PROBE].items() if k!='ddl'}):
                m._same_snapshot(spark,plan['snapshots'][m.PROBE])  # JSON round-trip safety
            with self.assertRaises(ValueError): self.prepare(directory,spark,snapshots)
            with patch.object(m,'_identity',return_value={'principal':'different'}):
                with self.assertRaises(ValueError): m._load(spark,directory)
            (directory/'restore_definitions.sql').write_text('modified')
            with patch.object(m,'_identity',return_value={'principal':'tester','application_id':'app'}):
                with self.assertRaises(ValueError): m._load(spark,directory)

    def test_probe_metadata_is_removed_before_canonical_registration(self):
        spark=Spark();plan={'snapshots':self.snapshots()}
        with tempfile.TemporaryDirectory() as root, \
             patch.object(m,'_load',return_value=(Path(root),plan)), \
             patch.object(m,'_names',return_value={'customers_lf_probe',*m.ENTITIES}), \
             patch.object(m,'_same_snapshot'), \
             patch.object(m,'inspect_external',side_effect=lambda s,n:plan['snapshots'][n]), \
             patch.object(m,'compare_v2',return_value={'rows':0}) as compare:
            m.migrate(spark,root)
            self.assertEqual(spark.calls[0],f'DROP TABLE {m.PROBE}')
            self.assertEqual(spark.calls[1],'DROP TABLE lakehouse_demo.bronze.customers')
            self.assertTrue(spark.calls[2].startswith('CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_demo.bronze.customers'))
            self.assertEqual(compare.call_count,4)
            self.assertFalse(any('PURGE' in x for x in spark.calls))

    def test_drift_stops_before_any_drop(self):
        spark=Spark()
        with patch.object(m,'_load',return_value=(Path('/tmp'),{'snapshots':self.snapshots()})), \
             patch.object(m,'_names',return_value={'customers_lf_probe',*m.ENTITIES}), \
             patch.object(m,'_same_snapshot',side_effect=ValueError('drift')):
            with self.assertRaises(ValueError): m.migrate(spark,'/tmp')
        self.assertEqual(spark.calls,[])

    def test_restore_handles_missing_objects_using_captured_ddl(self):
        spark=Spark();snapshots=self.snapshots()
        with patch.object(m,'_load',return_value=(Path('/tmp'),{'snapshots':snapshots})), \
             patch.object(m,'_names',return_value=set()):
            result=m.restore(spark,'/tmp')
        self.assertEqual(spark.calls,[s['ddl'] for s in snapshots.values()])
        self.assertEqual(result['source_objects_modified'],0)

    def test_inspection_rejects_managed_wrong_schema_or_location(self):
        class DataType:
            def __init__(self,t): self.t=t
            def simpleString(self): return self.t
        spark=Spark()
        spark.table=lambda name: SimpleNamespace(schema=SimpleNamespace(fields=[
            SimpleNamespace(name=n,dataType=DataType(t)) for n,t in columns('customers')]))
        good={'type':'EXTERNAL','provider':'csv','location':m.TARGET.location('customers')}
        for changed in [{'type':'MANAGED'},{'location':m.TARGET.location('orders')},{'provider':'delta'}]:
            with patch.object(m,'_details',return_value=good|changed):
                with self.assertRaises(ValueError): m.inspect_external(spark,m.PROBE)
        with patch.object(m,'_details',return_value=good),patch.object(m,'columns',return_value=[('wrong','string')]):
            with self.assertRaises(ValueError): m.inspect_external(spark,m.PROBE)

    def test_ddl_export_requires_exact_qualified_identity(self):
        spark=Spark()
        self.assertTrue(m._exact_ddl(spark,m.PROBE).startswith('CREATE EXTERNAL TABLE '+m.PROBE))
        spark.sql=lambda _: SimpleNamespace(collect=lambda:[('CREATE TABLE other.bronze.customers (id STRING)',)])
        with self.assertRaises(ValueError): m._exact_ddl(spark,m.PROBE)
