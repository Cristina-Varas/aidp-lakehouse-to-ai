"""Real local Delta/Gold reader test; only AIDP catalog metadata is simulated."""
from importlib.util import find_spec
import os
import tempfile
import unittest


@unittest.skipUnless(os.getenv('AIDP_RUN_TRAINING_SPARK')=='1' and find_spec('pandas'),
                     'Requires isolated Spark/Delta + training environment and explicit opt-in')
class TrainingSparkTests(unittest.TestCase):
    def test_read_only_pinned_gold_collection_and_open_aggregate(self):
        from datetime import datetime,timezone
        from decimal import Decimal
        from unittest.mock import patch
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (StructType,StructField,StringType,IntegerType,DecimalType,
                                       TimestampType,BooleanType,DateType)
        from aidp_lakehouse.gold import GoldConfig,CONTRACTS_GOLD,V2_PREFIX
        from aidp_lakehouse.aidp_bronze import BronzeConfig
        from aidp_lakehouse.training_contract import TrainingConfig
        from aidp_lakehouse.training_spark import read_training_inputs
        from test_training import fixture
        types={'string':StringType(),'int':IntegerType(),'decimal(6,3)':DecimalType(6,3),
               'timestamp':TimestampType(),'boolean':BooleanType(),'date':DateType()}
        with tempfile.TemporaryDirectory(prefix='aidp-training-reader-') as directory:
            spark=(SparkSession.builder.master('local[2]').appName('local-training-reader')
                .config('spark.jars',os.environ['AIDP_DELTA_JARS'])
                .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                .config('spark.sql.warehouse.dir',directory).config('spark.sql.session.timeZone','UTC')
                .config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1')
                .config('spark.ui.enabled','false').getOrCreate())
            spark.sparkContext.setLogLevel('ERROR')
            try:
                spark.sql('CREATE DATABASE gold')  # Isolated local fixture, never AIDP DDL.
                templates=fixture().to_dict('records')
                for entity,size in [('late_delivery_training',30000),('late_delivery_scoring',600)]:
                    contract=CONTRACTS_GOLD[entity]
                    rows=[]
                    for i in range(size):
                        row=dict(templates[i%len(templates)]);row['order_id']=f'O-{i:06d}'
                        if entity=='late_delivery_scoring':
                            row['accepted_at']=datetime(2026,10,22,tzinfo=timezone.utc)
                            row['promised_delivery_at_acceptance']=datetime(2026,10,27,tzinfo=timezone.utc)
                        values={}
                        for name,dtype in contract:
                            value=row[name]
                            if dtype=='timestamp' and hasattr(value,'to_pydatetime'):value=value.to_pydatetime()
                            if dtype=='decimal(6,3)':value=Decimal(str(value)).quantize(Decimal('0.001'))
                            values[name]=value
                        rows.append(values)
                    schema=StructType([StructField(n,types[t],True) for n,t in contract])
                    spark.createDataFrame(rows,schema).write.format('delta').saveAsTable('gold.'+entity)
                cfg=TrainingConfig(gold=GoldConfig(BronzeConfig(catalog='spark_catalog',prefix=V2_PREFIX)))
                statements=[];real=spark
                class ReadTrace:
                    def __getattr__(self,n):return getattr(real,n)
                    def sql(self,s):statements.append(s);return real.sql(s)
                with patch('aidp_lakehouse.training_spark.require_standard_catalog'):
                    data,first_open,snapshots=read_training_inputs(ReadTrace(),cfg)
                self.assertEqual(len(data),30000);self.assertTrue(first_open.startswith('2026-10-22T00:00:00'))
                self.assertEqual(str(data['accepted_at'].dt.tz),'UTC')
                self.assertEqual(snapshots['late_delivery_training']['delta_version'],0)
                self.assertEqual(snapshots['late_delivery_scoring']['delta_version'],0)
                self.assertEqual(snapshots['source_contract']['open_rows_scored'],0)
                self.assertTrue(all(s.startswith('DESCRIBE') for s in statements))
            finally:spark.stop()
