"""Real local Spark/Delta Gold fixtures. No AIDP/cloud sources are accessed."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest

from aidp_lakehouse.silver import transform_initial
from aidp_lakehouse.silver_spark import result_frames
from aidp_lakehouse.gold import CONTRACTS_GOLD, PREDICTORS
from aidp_lakehouse.gold_spark import (transform_frames,validate_inputs,validate_outputs,
                                      fingerprints,publish_initial,_same)


@unittest.skipUnless(os.getenv('AIDP_RUN_SPARK_TESTS')=='1','Requires isolated real Spark/Delta environment')
class GoldSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.tmp=tempfile.TemporaryDirectory(prefix='aidp-gold-test-')
        cls.spark=(SparkSession.builder.master('local[2]').appName('aidp-gold-local-tests')
                   .config('spark.jars',os.environ['AIDP_DELTA_JARS'])
                   .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                   .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                   .config('spark.sql.warehouse.dir',cls.tmp.name)
                   .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','1')
                   .config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        raw=json.loads((Path(__file__).parent/'fixtures/silver_initial.json').read_text())
        # Three delivered cases: late, exactly at promise, one second early; one open.
        original=copy.deepcopy(raw)
        for index,delivered in [(3,'2026-10-05T00:00:00Z'),(4,'2026-10-04T23:59:59Z')]:
            order=copy.deepcopy(original['orders'][0]);order['order_id']=f'O-{index:08d}'
            order['source_updated_at']='2026-10-05T01:00:00Z'
            order['current_priority']='URGENT'  # Deliberately differs from NORMAL acceptance.
            raw['orders'].append(order)
            delivery=copy.deepcopy(original['deliveries'][0]);delivery.update(order_id=order['order_id'],
                delivery_id='D-'+order['order_id'],dispatched_at='2026-10-03T00:00:00Z',delivered_at=delivered,
                source_created_at='2026-10-03T00:00:00Z',source_updated_at='2026-10-05T01:00:00Z',actual_carrier='CARRIER_C')
            raw['deliveries'].append(delivery)
            for old in original['events'][:3]:
                event=copy.deepcopy(old);event['order_id']=order['order_id'];event['event_id']=old['event_id']+str(index)
                at=order['accepted_at'] if event['event_type']=='ACCEPTED' else (delivery['dispatched_at'] if event['event_type']=='DISPATCHED' else delivered)
                event.update(event_at=at,source_created_at=at,source_updated_at=at)
                raw['events'].append(event)
        # Two incident events for the same order must not multiply operational rows.
        extra=copy.deepcopy(original['events'][3]);extra['event_id']+='-second';extra['payload']={'exception_type':'CAPACITY'}
        raw['events'].append(extra)
        result=transform_initial(raw)
        assert not result.quarantine,result.summary
        cls.inputs={n:f.cache() for n,f in result_frames(cls.spark,result).items() if n!='quarantine'}
        cls.outputs={n:f.cache() for n,f in transform_frames(cls.inputs).items()}

    @classmethod
    def tearDownClass(cls):
        for f in (*cls.outputs.values(),*cls.inputs.values()):f.unpersist()
        cls.spark.stop();cls.tmp.cleanup()

    def test_distributed_contracts_labels_boundaries_and_leakage(self):
        from pyspark.sql import functions as F
        validate_inputs(self.inputs,expected_counts={'customers':2,'orders':4,'deliveries':3,'events':12})
        summary=validate_outputs(self.inputs,self.outputs)
        self.assertEqual(summary['row_counts'],{'order_operations':4,'delivery_kpis':2,
                                              'late_delivery_training':3,'late_delivery_scoring':1})
        ops={r.order_id:r.asDict() for r in self.outputs['order_operations'].collect()}
        self.assertTrue(ops['O-00000001']['is_late'])
        self.assertFalse(ops['O-00000003']['is_late']);self.assertFalse(ops['O-00000004']['is_late'])
        self.assertIsNone(ops['O-00000002']['is_late'])
        self.assertEqual(ops['O-00000001']['incident_count'],2)
        self.assertEqual(ops['O-00000001']['incident_types'],['CAPACITY','TRANSPORT'])
        self.assertEqual(ops['O-00000001']['event_count'],5)
        self.assertEqual(self.outputs['order_operations'].filter(F.col('order_id')=='O-00000001').select(
            F.date_format('outcome_available_at','yyyy-MM-dd HH:mm:ss')).first()[0],'2026-10-12 01:00:00')
        row=self.outputs['late_delivery_training'].filter(F.col('order_id')=='O-00000003').first()
        self.assertEqual(row.priority_at_acceptance,'NORMAL');self.assertEqual(row.planned_carrier_at_acceptance,'CARRIER_A')
        totals=self.outputs['delivery_kpis'].agg(F.sum('deliveries'),F.sum('late_deliveries')).first()
        self.assertEqual(tuple(totals),(3,1))
        # Later-only attribute changes leave model projections identical.
        altered=dict(self.inputs)
        altered['orders']=self.inputs['orders'].withColumn('current_priority',F.lit('HIGH'))
        altered['customers']=self.inputs['customers'].withColumn('segment',F.lit('PREMIUM'))
        altered['deliveries']=self.inputs['deliveries'].withColumn('actual_carrier',F.lit('CARRIER_B'))
        other=transform_frames(altered)
        for n in ('late_delivery_training','late_delivery_scoring'):_same(other[n],self.outputs[n],n)

    def test_invalid_duplicates_references_labels_and_nulls_are_rejected(self):
        from pyspark.sql import functions as F
        bad=dict(self.inputs);bad['deliveries']=bad['deliveries'].unionByName(bad['deliveries'])
        with self.assertRaisesRegex(ValueError,'duplicate key'):validate_inputs(bad)
        bad=dict(self.inputs);bad['orders']=bad['orders'].withColumn('customer_id',F.lit('UNKNOWN'))
        with self.assertRaisesRegex(ValueError,'unknown customer_id'):validate_inputs(bad)
        wrong=dict(self.outputs);wrong['order_operations']=wrong['order_operations'].withColumn('is_late',F.lit(False))
        with self.assertRaisesRegex(ValueError,'label/nullability'):validate_outputs(self.inputs,wrong)
        wrong=dict(self.outputs);wrong['late_delivery_training']=wrong['late_delivery_training'].withColumn('units',F.lit(None).cast('int'))
        with self.assertRaisesRegex(ValueError,'unexpected null'):validate_outputs(self.inputs,wrong)

    def test_managed_delta_initial_rerun_and_all_target_compatibility(self):
        from pyspark.sql import functions as F
        self.spark.sql('CREATE DATABASE gold_fixture')
        before=fingerprints(self.outputs)
        first=publish_initial(self.spark,self.outputs,'gold_fixture',forbidden_source_root='oci://fixture@namespace/source')
        again={n:f.cache() for n,f in transform_frames(self.inputs).items()}
        try:
            for n in again:_same(self.outputs[n],again[n],n)
            self.assertEqual(before,fingerprints(again))
            second=publish_initial(self.spark,again,'gold_fixture',forbidden_source_root='oci://fixture@namespace/source')
            self.assertEqual(first['order_operations']['inserted_rows'],4)
            self.assertTrue(all(v['inserted_rows']==0 for v in second.values()))
        finally:
            for f in again.values():f.unpersist()
        changed=dict(self.outputs)
        changed['late_delivery_scoring']=changed['late_delivery_scoring'].withColumn('units',F.col('units')+1)
        with self.assertRaisesRegex(ValueError,'changed or extra'):publish_initial(self.spark,changed,'gold_fixture',forbidden_source_root='oci://fixture@namespace/source')
        self.spark.sql('CREATE DATABASE gold_incompatible')
        self.spark.sql('CREATE TABLE gold_incompatible.late_delivery_scoring (order_id STRING) USING PARQUET')
        with self.assertRaisesRegex(ValueError,'not managed Delta'):publish_initial(self.spark,self.outputs,'gold_incompatible',forbidden_source_root='oci://fixture@namespace/source')
        self.assertEqual([r.tableName for r in self.spark.sql('SHOW TABLES IN gold_incompatible').collect()],['late_delivery_scoring'])

    def test_real_delta_version_pinning_for_silver_reader(self):
        from dataclasses import asdict
        from types import SimpleNamespace
        from unittest.mock import patch
        from pyspark.sql import functions as F
        from aidp_lakehouse.gold import GoldConfig
        from aidp_lakehouse.gold_spark import read_silver
        from aidp_lakehouse.silver_spark import spark_schema
        self.spark.sql('CREATE DATABASE gold_silver_pinning')
        cfg=GoldConfig()
        expected={n:f.count() for n,f in self.inputs.items()}
        manifest={'row_counts':{cfg.bronze.file_uri(n).rsplit('/',1)[1]:v for n,v in expected.items()}}
        for entity,frame in self.inputs.items():
            frame.withColumn('_source_file',F.lit(cfg.bronze.file_uri(entity))).write.format('delta').saveAsTable('gold_silver_pinning.'+entity)
        self.spark.createDataFrame([],spark_schema('quarantine')).write.format('delta').saveAsTable('gold_silver_pinning.quarantine')
        local=SimpleNamespace(bronze=cfg.bronze,silver_namespace='gold_silver_pinning')
        # Only AIDP catalog/control lookup is simulated; Delta HISTORY/version reads are real.
        with patch('aidp_lakehouse.gold_spark.require_standard_catalog'), \
             patch('aidp_lakehouse.gold_spark.read_control_documents',return_value=(manifest,{})), \
             patch('aidp_lakehouse.gold_spark.EXPECTED_SILVER',expected):
            pinned,counts=read_silver(self.spark,local)
        try:
            self.assertEqual(counts,expected)
            before=pinned['customers'].count()
            self.inputs['customers'].withColumn('customer_id',F.concat(F.lit('NEW-'),F.col('customer_id'))).write.format('delta').mode('append').saveAsTable('gold_silver_pinning.customers')
            self.assertEqual(pinned['customers'].count(),before)
            self.assertEqual(self.spark.table('gold_silver_pinning.customers').count(),before+2)
        finally:
            for f in pinned.values():f.unpersist()
