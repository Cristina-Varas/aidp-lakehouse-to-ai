"""Pinned Gold input, bounded inference and guarded insert-only managed Delta output."""
from functools import reduce
from .aidp_bronze import _details,identifier,require_standard_catalog
from .gold import CONTRACTS_GOLD
from .gold_spark import _schema,_unique,_same,_fail_if
from .scoring import PREDICTION_CONTRACT,score_orders


def read_scoring_input(spark,config):
    import pandas as pd
    from pyspark.sql import functions as F
    spark.conf.set('spark.sql.session.timeZone','UTC')
    require_standard_catalog(spark,config.gold.bronze)
    name=config.input_table
    detail=_details(spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect())
    if detail.get('type','').upper()!='MANAGED' or detail.get('provider','').lower()!='delta':
        raise ValueError('Gold scoring input must remain managed Delta')
    identity=spark.sql(f'DESCRIBE DETAIL {name}').first().asDict()
    version=config.input_delta_version
    if version is None:version=int(spark.sql(f'DESCRIBE HISTORY {name} LIMIT 1').first()['version'])
    frame=spark.read.format('delta').option('versionAsOf',version).table(name)
    _schema(frame,CONTRACTS_GOLD['late_delivery_scoring'],name);_unique(frame,('order_id',),name)
    _fail_if(frame,reduce(lambda a,b:a|b,(F.col(n).isNull() for n in frame.columns)),'Gold scoring contains null values')
    if frame.count()!=600:raise ValueError('Expected exactly 600 Gold open orders')
    fields=[F.date_format(F.col(n),"yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias(n)
            if t=='timestamp' else F.col(n) for n,t in CONTRACTS_GOLD['late_delivery_scoring']]
    rows=frame.select(*fields).orderBy('order_id').limit(601).collect()
    data=pd.DataFrame.from_records([r.asDict() for r in rows],columns=frame.columns)
    if len(data)!=600:raise ValueError('Scoring cardinality changed during fixed-snapshot read')
    if spark.sql(f'DESCRIBE DETAIL {name}').first()['id']!=identity['id']:
        raise ValueError('Gold scoring table identity changed during snapshot collection')
    snapshot={'table':name,'table_id':identity['id'],'delta_version':int(version),
              'schema':[list(pair) for pair in CONTRACTS_GOLD['late_delivery_scoring']]}
    return data,snapshot


def prediction_frame(spark,result):
    """Explicit types and aware UTC conversion; no Spark/pandas Arrow coupling."""
    from pyspark.sql.types import (StructType,StructField,StringType,TimestampType,
                                  DoubleType,BooleanType,LongType,DateType)
    types={'string':StringType(),'timestamp':TimestampType(),'double':DoubleType(),
           'boolean':BooleanType(),'bigint':LongType(),'date':DateType()}
    schema=StructType([StructField(n,types[t],False) for n,t in PREDICTION_CONTRACT])
    rows=[]
    for record in result.to_dict('records'):
        row=[]
        for name,dtype in PREDICTION_CONTRACT:
            value=record[name]
            if dtype=='timestamp':value=value.to_pydatetime()
            elif dtype=='date':value=value.date()
            elif dtype=='double':value=float(value)
            elif dtype=='bigint':value=int(value)
            elif dtype=='boolean':value=bool(value)
            row.append(value)
        rows.append(tuple(row))
    return spark.createDataFrame(rows,schema)


def _target(spark,config):
    name=config.output_table
    detail=_details(spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect())
    if detail.get('type','').upper()!='MANAGED' or detail.get('provider','').lower()!='delta':
        raise ValueError('Predictions target must be managed Delta; existing object preserved')
    location=detail.get('location','').rstrip('/')
    protected=config.gold.bronze.root_uri.split('/source-data/')[0]+'/source-data'
    if not location or location==protected or location.startswith(protected+'/'):
        raise ValueError('Predictions location is missing or overlaps protected source files')
    existing=spark.table(name)
    _schema(existing,PREDICTION_CONTRACT,name);_unique(existing,('order_id',),name)
    return existing


def publish_predictions(spark,planned,config):
    from uuid import uuid4
    from pyspark.sql import functions as F
    for part in config.output_table.split('.'):identifier(part)
    _schema(planned,PREDICTION_CONTRACT,'predictions');_unique(planned,('order_id',),'predictions')
    if planned.count()!=600:raise ValueError('Refuse partial prediction publication')
    _fail_if(planned,reduce(lambda a,b:a|b,(F.col(n).isNull() for n,_ in PREDICTION_CONTRACT)),
             'Predictions contain null values')
    _fail_if(planned,F.isnan('late_probability') | (F.col('late_probability')<0) | (F.col('late_probability')>1) |
             F.isnan('decision_threshold') | (F.col('decision_threshold')<0) | (F.col('decision_threshold')>1) |
             (F.col('predicted_late')!=(F.col('late_probability')>=F.col('decision_threshold'))),
             'Invalid prediction probability/decision')
    visible={row['tableName'] for row in spark.sql(f'SHOW TABLES IN {config.gold.gold_namespace}').collect()}
    name=config.output_table
    before=0
    if 'late_delivery_predictions' in visible:
        existing=_target(spark,config)
        if existing.exceptAll(planned).limit(1).count():
            raise ValueError('Existing predictions contain changed or extra rows; preserved without mutation')
        before=existing.count()
    else:
        fields=', '.join(f'{n} {t.upper()}' for n,t in PREDICTION_CONTRACT)
        spark.sql(f'CREATE TABLE IF NOT EXISTS {name} ({fields}) USING DELTA')
    # Recheck after possible CREATE race and before insert-only merge.
    existing=_target(spark,config)
    if existing.exceptAll(planned).limit(1).count():raise ValueError('Predictions changed before MERGE; preserved')
    view='scoring_predictions_'+uuid4().hex
    planned.createOrReplaceTempView(view)
    try:
        fields=', '.join(n for n,_ in PREDICTION_CONTRACT)
        values=', '.join('s.'+n for n,_ in PREDICTION_CONTRACT)
        spark.sql(f'MERGE INTO {name} t USING {view} s ON t.order_id = s.order_id '
                  f'WHEN NOT MATCHED THEN INSERT ({fields}) VALUES ({values})')
    finally:
        spark.catalog.dropTempView(view)
    actual=_target(spark,config)
    _same(actual,planned,'Published predictions')
    return {'rows':actual.count(),'inserted_rows':actual.count()-before}


def score_publish_repeat(spark,data,bundle,config,snapshot):
    """Only return PASS after two complete inferences/publications compare equal."""
    first=second=None
    try:
        values,summary=score_orders(data,bundle,config,snapshot)
        first=prediction_frame(spark,values).cache()
        publication=publish_predictions(spark,first,config)
        repeated,again=score_orders(data,bundle,config,snapshot)
        second=prediction_frame(spark,repeated).cache()
        _same(first,second,'Repeated full-row scoring')
        if summary!=again:raise ValueError('Repeated scoring summary differs')
        repetition=publish_predictions(spark,second,config)
        if repetition['inserted_rows']!=0:raise ValueError('Repeated scoring inserted additional rows')
        return {'status':'PASS','scoring':summary,'publication':publication,'repeat':repetition,
                'full_row_repeat_equal':True}
    finally:
        for frame in (first,second):
            if frame is not None:frame.unpersist()
