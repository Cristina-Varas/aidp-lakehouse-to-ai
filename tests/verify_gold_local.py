"""Local full initial fixture verification only; no cloud clients or table writes."""
import argparse,csv,json,os
from pathlib import Path
from collections import defaultdict
from pyspark.sql import SparkSession
from aidp_lakehouse import __version__
from aidp_lakehouse.silver import transform_initial
from aidp_lakehouse.silver_spark import result_frames
from aidp_lakehouse.gold_spark import transform_frames,validate_inputs,validate_outputs,fingerprints,_same

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source-dir',type=Path,required=True,help='Existing local LF v2 initial directory')
parser.add_argument('--report',type=Path,required=True,help='Aggregate verification report; no source records')
args=parser.parse_args()
source=args.source_dir.expanduser().resolve()
raw={}
for entity in ('customers','orders','deliveries','events'):
    path=source/(entity+('.jsonl' if entity=='events' else '.csv'))
    with path.open(newline='') as f:
        rows=[json.loads(line) for line in f] if entity=='events' else list(csv.DictReader(f))
    raw[entity]=[{**r,'_source_file':path.as_uri()} for r in rows]
result=transform_initial(raw)
assert not result.quarantine
spark=(SparkSession.builder.master('local[2]').appName('full-local-initial-gold')
       .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','4')
       .config('spark.ui.enabled','false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
inputs={n:f.cache() for n,f in result_frames(spark,result).items() if n!='quarantine'}
outputs={};again={}
try:
    counts=validate_inputs(inputs,expected_counts={'customers':600,'orders':30600,'deliveries':30000,'events':94572})
    outputs={n:f.cache() for n,f in transform_frames(inputs).items()}
    summary=validate_outputs(inputs,outputs)
    digest=fingerprints(outputs)
    again={n:f.cache() for n,f in transform_frames(inputs).items()}
    for n in outputs:_same(outputs[n],again[n],n)
    assert digest==fingerprints(again)
    # Independent, local test oracle from the typed Silver dictionaries.
    orders={r['order_id']:r for r in result.tables['orders']}
    oracle=defaultdict(lambda:[0,0])
    for d in result.tables['deliveries']:
        o=orders[d['order_id']]
        key=(d['delivered_at'].date().replace(day=1).isoformat(),o['customer_region_at_acceptance'],d['actual_carrier'])
        oracle[key][0]+=1;oracle[key][1]+=int(d['delivered_at']>o['promised_delivery_at_acceptance'])
    actual={(r['period_start'].isoformat(),r['customer_region_at_acceptance'],r['actual_carrier']):[r['deliveries'],r['late_deliveries']]
            for r in outputs['delivery_kpis'].collect()}
    assert dict(oracle)==actual
    report={'environment':'real local Spark 3.5.6; no AIDP/OCI calls; no table publication',
            'date':'2026-10-30','package_version':__version__,'reference_date':'2026-10-28','seed':42,'source_dir':str(source),
            'silver_rows':counts,'gold':summary,'fingerprints':digest,
            'identical_second_transform':True,'all_columns_exceptAll_both_directions':True,
            'independent_python_kpi_oracle':'PASS','kpi_groups':len(actual),
            'business_row_collection':'Only the independent local test oracle; production Gold uses distributed transforms'}
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
finally:
    for f in (*outputs.values(),*again.values(),*inputs.values()):f.unpersist()
    spark.stop()
