"""End-to-end LOCAL training benchmark: local LF files, local Delta, local MLflow.

Never authenticates to AIDP/OCI; never scores the open cohort. Dataset/model
artifacts must remain outside Git. Use a NEW --work-dir for each run.
"""
import argparse,csv,json
from dataclasses import replace
from pathlib import Path
import tempfile
from unittest.mock import patch
from pyspark.sql import SparkSession
from aidp_lakehouse import __version__
from aidp_lakehouse.aidp_bronze import BronzeConfig
from aidp_lakehouse.gold import GoldConfig,V2_PREFIX
from aidp_lakehouse.silver import transform_initial
from aidp_lakehouse.silver_spark import result_frames
from aidp_lakehouse.gold_spark import transform_frames
from aidp_lakehouse.training import train_and_evaluate
from aidp_lakehouse.training_contract import TrainingConfig
from aidp_lakehouse.training_spark import read_training_inputs
from aidp_lakehouse.training_mlflow import preflight,run_experiment
import mlflow

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source-dir',type=Path,required=True)
parser.add_argument('--work-dir',type=Path,required=True)
parser.add_argument('--report',type=Path,required=True)
args=parser.parse_args()
source=args.source_dir.expanduser().resolve();work=args.work_dir.expanduser().resolve()
repo=Path(__file__).resolve().parents[1]
if work==repo or repo in work.parents:raise ValueError('Local models and SQLite data must stay outside Git')
work.mkdir(parents=True,exist_ok=False)
manifest=json.loads((source/'manifest.json').read_text())
assert manifest['config']['seed']==42 and manifest['config']['reference_date']=='2026-10-28'
raw={}
for entity in ('customers','orders','deliveries','events'):
    path=source/(entity+('.jsonl' if entity=='events' else '.csv'))
    with path.open(newline='') as stream:
        rows=[json.loads(line) for line in stream] if entity=='events' else list(csv.DictReader(stream))
    raw[entity]=[{**row,'_source_file':path.as_uri()} for row in rows]
silver=transform_initial(raw)
if silver.quarantine:raise ValueError('Unexpected local source quarantine')
spark=(SparkSession.builder.master('local[2]').appName('local-late-delivery-training')
    .config('spark.jars',__import__('os').environ['AIDP_DELTA_JARS'])
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.sql.warehouse.dir',str(work/'warehouse')).config('spark.sql.session.timeZone','UTC')
    .config('spark.sql.shuffle.partitions','2').config('spark.databricks.delta.snapshotPartitions','1')
    .config('spark.ui.enabled','false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    spark.sql('CREATE DATABASE gold')  # Local fixture only; never an AIDP catalog substitute.
    inputs={n:f for n,f in result_frames(spark,silver).items() if n!='quarantine'}
    gold=transform_frames(inputs)
    for entity in ('late_delivery_training','late_delivery_scoring'):
        gold[entity].write.format('delta').saveAsTable('gold.'+entity)
    cfg=TrainingConfig(gold=GoldConfig(BronzeConfig(catalog='spark_catalog',prefix=V2_PREFIX)))
    # Only AIDP-specific catalog identity lookup is simulated. Delta reads are real.
    with patch('aidp_lakehouse.training_spark.require_standard_catalog'):
        data,first_open,snapshots=read_training_inputs(spark,cfg)
    uri='sqlite:///'+str(work/'mlflow.db')
    mlflow.set_tracking_uri(uri);mlflow.set_registry_uri(uri)  # EXPLICIT LOCAL test adapter only.
    mlflow.create_experiment(cfg.experiment_name,artifact_location=(work/'artifacts').as_uri())
    capabilities=preflight(cfg,require_aidp=False)
    result,registry=run_experiment(data,cfg,first_open,snapshots,capabilities)
    # Repeat actual sklearn fitting to verify deterministic selection and probabilities.
    repeat=train_and_evaluate(data,cfg,first_open)
    import numpy as np
    if result.report!=repeat.report:raise ValueError('Local repeat changed reported training results')
    np.testing.assert_array_equal(result.verification_probabilities,repeat.verification_probabilities)
    report=dict(date='2026-10-31',package_version=__version__,environment='LOCAL Spark/Delta + sklearn + SQLite MLflow; not AIDP',
                source_dir=str(source),runtime=capabilities,input_snapshots=snapshots,
                evaluation=result.report,registry=registry,reproducible_repeat=True,
                open_orders_scored=0,aidp_writes=False,
                simulated_checks=['AIDP catalog identity lookup only; local spark_catalog used'])
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'selected':result.report['selected_candidate'],'threshold':result.report['selected_threshold'],
                      'test':result.report['test'],'partitions':result.report['partitions'],
                      'registry':registry,'open_orders_scored':0},indent=2))
finally:
    spark.stop()
