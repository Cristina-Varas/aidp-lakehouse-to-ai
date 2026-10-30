"""Opt-in tests using real Spark/Delta, never AIDP or Object Storage."""
import json
import os
from pathlib import Path
import tempfile
import unittest

from aidp_lakehouse.silver import transform_initial
from aidp_lakehouse.silver_spark import publish_initial, result_frames, _preflight_rows


@unittest.skipUnless(os.getenv('AIDP_RUN_SPARK_TESTS')=='1','Real Spark tests require the isolated Spark 3.5/Delta 3.2 environment')
class RealSparkSilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.tmp=tempfile.TemporaryDirectory(prefix='aidp-real-silver-')
        jars=os.environ['AIDP_DELTA_JARS']
        cls.spark=(SparkSession.builder.master('local[2]').appName('aidp-initial-silver-tests')
                   .config('spark.jars',jars)
                   .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                   .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                   .config('spark.sql.warehouse.dir',cls.tmp.name)
                   .config('spark.sql.session.timeZone','UTC')
                   .config('spark.sql.shuffle.partitions','1')
                   .config('spark.databricks.delta.snapshotPartitions','1')
                   .config('spark.ui.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop();cls.tmp.cleanup()

    def fixture(self):
        return json.loads((Path(__file__).parent/'fixtures/silver_initial.json').read_text())

    def test_real_managed_delta_initial_rerun_and_drift_rejection(self):
        from pyspark.sql import functions as F
        namespace='silver_managed_test'
        self.spark.sql(f'CREATE DATABASE {namespace}')  # Local test catalog only; not AIDP DDL.
        result=transform_initial(self.fixture())
        first=publish_initial(self.spark,result,namespace)
        self.assertEqual({k:v['resulting_silver_rows'] for k,v in first.items()},
                         {'customers':2,'orders':2,'deliveries':1,'events':5,'quarantine':0})
        again=publish_initial(self.spark,transform_initial(self.fixture()),namespace)
        self.assertTrue(all(v['inserted_rows']==0 for v in again.values()))
        self.assertEqual(first['orders']['data_types']['accepted_at'],'timestamp')
        self.assertEqual(first['orders']['data_types']['inventory_fill_rate_at_acceptance'],'decimal(6,3)')
        self.assertEqual(first['events']['data_types']['payload'],'map<string,string>')
        # A changed existing record is not overwritten, updated, or silently accepted.
        changed=transform_initial(self.fixture())
        changed.tables['customers'][0]['customer_name']='Unexpected replacement'
        with self.assertRaisesRegex(ValueError,'changed or extra'):
            publish_initial(self.spark,changed,namespace)
        actual=self.spark.table(f'{namespace}.customers').filter(F.col('customer_id')=='C-000001').first()
        self.assertNotEqual(actual['customer_name'],'Unexpected replacement')
        # A same-schema external/non-Delta table must not be treated as managed Delta.
        self.spark.sql('CREATE DATABASE silver_incompatible_test')
        self.spark.sql('CREATE TABLE silver_incompatible_test.events (event_id STRING) USING PARQUET')
        with self.assertRaisesRegex(ValueError,'not managed Delta'):
            publish_initial(self.spark,result,'silver_incompatible_test')
        names=[r.tableName for r in self.spark.sql('SHOW TABLES IN silver_incompatible_test').collect()]
        self.assertEqual(names,['events'])  # No other outputs created before detecting conflict.

    def test_real_spark_typed_quarantine_and_key_checks(self):
        from pyspark.sql import functions as F
        raw=self.fixture();raw['orders'][0]['units']='invalid';raw['orders'][0]['accepted_at']='2026-02-30T00:00:00Z'
        result=transform_initial(raw)
        frames=result_frames(self.spark,result)
        rejected=frames['quarantine'].filter(F.col('entity')=='orders').collect()
        self.assertEqual(len(rejected),1)
        self.assertEqual(rejected[0]['occurrence_count'],1)
        self.assertGreaterEqual(len(rejected[0]['rejection_reasons']),2)
        self.assertIn('2026-02-30',rejected[0]['raw_record'])
        duplicated=frames['customers'].unionByName(frames['customers'])
        with self.assertRaisesRegex(ValueError,'duplicate keys'):
            _preflight_rows(duplicated,frames['customers'],'customers')


if __name__=='__main__':unittest.main()
