"""Real local registry -> Gold snapshot -> managed Delta publication and repeat.

Only AIDP catalog identity is mocked. Uses a local fitted fixture, not AIDP's model.
"""
from importlib.util import find_spec
import os
import tempfile
import unittest


@unittest.skipUnless(os.getenv('AIDP_RUN_SCORING_SPARK')=='1' and find_spec('mlflow'),
                     'Requires isolated real Spark/Delta/MLflow environment and explicit opt-in')
class ScoringIntegrationTests(unittest.TestCase):
    def test_registry_pinned_input_publish_repeat_and_protection(self):
        from dataclasses import replace
        from decimal import Decimal
        from pathlib import Path
        from unittest.mock import patch
        import mlflow
        import numpy as np
        from pyspark.sql import SparkSession,functions as F
        from pyspark.sql.types import (StructType,StructField,StringType,IntegerType,DecimalType,
                                       TimestampType,DateType)
        from aidp_lakehouse.gold import GoldConfig,CONTRACTS_GOLD,V2_PREFIX
        from aidp_lakehouse.aidp_bronze import BronzeConfig
        from aidp_lakehouse.scoring import ScoringConfig
        from aidp_lakehouse.scoring_mlflow import load_verified_pipeline
        from aidp_lakehouse.scoring_spark import read_scoring_input,score_publish_repeat,publish_predictions
        from aidp_lakehouse.training_contract import TrainingConfig
        from aidp_lakehouse.training_mlflow import preflight,run_experiment
        from test_training import fixture
        from test_scoring import open_fixture,saved_fixture
        types={'string':StringType(),'int':IntegerType(),'decimal(6,3)':DecimalType(6,3),
               'timestamp':TimestampType(),'date':DateType()}
        with tempfile.TemporaryDirectory(prefix='aidp-scoring-integration-') as directory:
            spark=(SparkSession.builder.master('local[2]').appName('local-exact-version-scoring')
                .config('spark.jars',os.environ['AIDP_DELTA_JARS'])
                .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                .config('spark.sql.warehouse.dir',str(Path(directory)/'warehouse'))
                .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','1')
                .config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').getOrCreate())
            spark.sparkContext.setLogLevel('ERROR')
            tracking,registry=mlflow.get_tracking_uri(),mlflow.get_registry_uri()
            try:
                spark.sql('CREATE DATABASE gold')
                gold=GoldConfig(BronzeConfig(catalog='spark_catalog',prefix=V2_PREFIX))
                cfg=ScoringConfig(gold=gold)
                contract=CONTRACTS_GOLD['late_delivery_scoring']
                rows=[]
                for record in open_fixture().to_dict('records'):
                    row=[]
                    for name,dtype in contract:
                        value=record[name]
                        if dtype=='timestamp':value=value.to_pydatetime()
                        elif dtype=='decimal(6,3)':value=Decimal(str(value)).quantize(Decimal('.001'))
                        row.append(value)
                    rows.append(tuple(row))
                schema=StructType([StructField(n,types[t],False) for n,t in contract])
                spark.createDataFrame(rows,schema).write.format('delta').saveAsTable(cfg.input_table)
                with patch('aidp_lakehouse.scoring_spark.require_standard_catalog'):
                    data,snapshot=read_scoring_input(spark,cfg)
                self.assertEqual(len(data),600);self.assertEqual(snapshot['delta_version'],0)
                # An independent source commit after collection must not change pinned data.
                spark.table(cfg.input_table).limit(1).write.format('delta').mode('append').saveAsTable(cfg.input_table)
                cfg=replace(cfg,input_delta_version=0)
                with patch('aidp_lakehouse.scoring_spark.require_standard_catalog'):
                    pinned,pinned_snapshot=read_scoring_input(spark,cfg)
                self.assertEqual(len(pinned),600);self.assertEqual(pinned_snapshot,snapshot)
                with patch('aidp_lakehouse.scoring_spark.require_standard_catalog'):
                    with self.assertRaises(ValueError):read_scoring_input(spark,replace(cfg,input_delta_version=1))
                uri='sqlite:///'+str(Path(directory)/'mlflow.db')
                mlflow.set_tracking_uri(uri);mlflow.set_registry_uri(uri)  # Explicit LOCAL backend, never native configuration.
                train_cfg=TrainingConfig(gold=gold)
                mlflow.create_experiment(train_cfg.experiment_name,artifact_location=(Path(directory)/'artifacts').as_uri())
                snapshots=saved_fixture(cfg)[1]['input_delta_versions.json']
                result,registered=run_experiment(fixture(),train_cfg,'2026-10-22T00:00:00Z',snapshots,
                                                 preflight(train_cfg,require_aidp=False))
                cfg=replace(cfg,training_run_id=registered['run_id'])
                bundle,caps=load_verified_pipeline(cfg,require_aidp=False)
                self.assertEqual(caps['environment'],'explicit local test backend')
                with patch.object(bundle.pipeline,'fit',side_effect=AssertionError('Scoring must never fit')):
                    proof=score_publish_repeat(spark,data,bundle,cfg,snapshot)
                    rerun=score_publish_repeat(spark,data,bundle,cfg,snapshot)
                self.assertEqual(proof['status'],'PASS');self.assertEqual(proof['publication']['inserted_rows'],600)
                self.assertEqual(proof['repeat']['inserted_rows'],0)
                self.assertEqual(rerun['publication']['inserted_rows'],0)
                self.assertEqual(proof['scoring'],rerun['scoring'])
                actual=spark.table(cfg.output_table).cache()
                self.assertEqual(actual.count(),600)
                self.assertEqual(actual.select('input_table_id').first()[0],snapshot['table_id'])
                original=actual.collect()
                wrong=actual.withColumn('late_probability',F.lit(.99)).withColumn('predicted_late',F.lit(True))
                with self.assertRaisesRegex(ValueError,'changed or extra'):publish_predictions(spark,wrong,cfg)
                self.assertEqual(set(map(str,original)),set(map(str,spark.table(cfg.output_table).collect())))
                # Wrong type/provider, schema and unrelated contents are preserved, never overwritten.
                detached=spark.createDataFrame(original,actual.schema)
                actual.unpersist()
                spark.sql(f'ALTER TABLE {cfg.output_table} RENAME TO gold.kept_predictions')
                spark.sql(f'CREATE VIEW {cfg.output_table} AS SELECT order_id FROM gold.kept_predictions')
                with self.assertRaisesRegex(ValueError,'managed Delta'):publish_predictions(spark,detached,cfg)
                spark.sql(f'DROP VIEW {cfg.output_table}')  # Isolated fixture cleanup only.
                spark.sql(f'CREATE TABLE {cfg.output_table} (order_id STRING) USING DELTA')
                with self.assertRaisesRegex(ValueError,'schema'):publish_predictions(spark,detached,cfg)
                wrong_cfg=replace(cfg,training_run_id='a'*32)
                with self.assertRaisesRegex(ValueError,'different model or training run'):
                    load_verified_pipeline(wrong_cfg,require_aidp=False)
                print('LOCAL SCORING INTEGRATION PASS: exact registry reload, 600 predictions, repeat +0, protected conflicts')
            finally:
                if mlflow.active_run():mlflow.end_run(status='FAILED')
                mlflow.set_tracking_uri(tracking);mlflow.set_registry_uri(registry)
                spark.stop()
