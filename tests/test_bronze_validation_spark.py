"""Opt-in real local Spark full comparison. Uses temporary local files, never AIDP."""
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from aidp_lakehouse.aidp_bronze import columns, reader_options, literal
from aidp_lakehouse.bronze_validation import compare_table, comparison_frame


@unittest.skipUnless(os.getenv('AIDP_RUN_SPARK_TESTS')=='1','Requires isolated real Spark environment')
class ComparisonSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.tmp=tempfile.TemporaryDirectory(prefix='bronze-compare-')
        cls.spark=(SparkSession.builder.master('local[2]').appName('local-bronze-comparison')
                   .config('spark.sql.warehouse.dir',cls.tmp.name+'/warehouse')
                   .config('spark.sql.shuffle.partitions','1').config('spark.ui.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.spark.sql('CREATE DATABASE bronze_comparison_fixture')

    @classmethod
    def tearDownClass(cls):
        cls.spark.sql('DROP DATABASE bronze_comparison_fixture CASCADE')  # Only this local test database.
        cls.spark.stop();cls.tmp.cleanup()

    def test_all_columns_multiset_maps_whitespace_and_version(self):
        import csv
        from pyspark.sql import functions as F
        for entity in ('customers','events'):
            folder=Path(self.tmp.name)/entity;folder.mkdir()
            path=folder/(entity+('.jsonl' if entity=='events' else '.csv'))
            if entity=='customers':
                record=['C1',' Example, Operations ','EU','SMB','2026-10-01T00:00:00Z','2026-10-02T00:00:00Z','1']
                with path.open('w',newline='') as out:
                    writer=csv.writer(out,lineterminator='\n');writer.writerow([n for n,_ in columns(entity)[:-1]])
                    writer.writerows([record,record])
            else:
                record=dict(event_id='E1',order_id='O1',event_type='ACCEPTED',event_at='2026-10-01T00:00:00Z',
                            payload={'b':' value ','a':None},row_version=1,
                            source_created_at='2026-10-01T00:00:00Z',source_updated_at='2026-10-01T00:00:00Z')
                path.write_text((json.dumps(record)+'\n')*2)
            name='bronze_comparison_fixture.'+entity
            options=', '.join(k+'='+literal(v) for k,v in reader_options(entity).items())
            schema=', '.join(n+' '+t for n,t in columns(entity))
            self.spark.sql(f'CREATE TABLE {name} ({schema}) USING '+('JSON' if entity=='events' else 'CSV')+
                           f' OPTIONS ({options}) LOCATION '+literal(folder.as_uri()))
            details={r[0]:r[1] for r in self.spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect()}
            cfg=SimpleNamespace(table=lambda e:name,location=lambda e:details['Location'],file_uri=lambda e:path.as_uri())
            result=compare_table(self.spark,cfg,entity,expected_count=2)
            self.assertEqual(result['registered_minus_direct'],0)
            self.assertEqual(result['direct_minus_registered'],0)
            with self.assertRaisesRegex(ValueError,'row counts'):
                compare_table(self.spark,cfg,entity,expected_count=3)
            if entity=='events':
                frame=self.spark.table(name)
                a=comparison_frame(frame,entity)
                b=comparison_frame(frame.withColumn('payload',F.create_map(F.lit('different'),F.lit('x'))),entity)
                self.assertEqual(a.exceptAll(b).count(),2)
                self.assertEqual(a.exceptAll(a.limit(1)).count(),1)  # Duplicate multiplicity preserved.
                null_payload=comparison_frame(frame.withColumn('payload',F.lit(None).cast('map<string,string>')),entity)
                self.assertEqual(a.exceptAll(null_payload).count(),2)
            else:
                self.assertEqual(self.spark.table(name).first()['customer_name'],' Example, Operations ')
