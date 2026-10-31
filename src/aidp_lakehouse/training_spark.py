"""Read-only, version-pinned Gold adapter. Open orders supply aggregate cutoffs only."""
from .aidp_bronze import _details,require_standard_catalog
from .gold import CONTRACTS_GOLD,PREDICTORS
from .gold_spark import _schema,_unique


def read_training_inputs(spark,config):
    from pyspark.sql import functions as F
    import pandas as pd
    spark.conf.set('spark.sql.session.timeZone','UTC')
    require_standard_catalog(spark,config.gold.bronze)
    namespace=config.gold.gold_namespace
    snapshots={};frames={}
    for entity in ('late_delivery_training','late_delivery_scoring'):
        name=f'{namespace}.{entity}'
        detail=_details(spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect())
        if detail.get('type','').upper()!='MANAGED' or detail.get('provider','').lower()!='delta':
            raise ValueError(f'{name} must remain managed Delta')
        version=int(spark.sql(f'DESCRIBE HISTORY {name} LIMIT 1').first()['version'])
        frame=spark.read.format('delta').option('versionAsOf',version).table(name)
        _schema(frame,CONTRACTS_GOLD[entity],entity);_unique(frame,('order_id',),entity)
        snapshots[entity]={'table':name,'delta_version':version,'schema':CONTRACTS_GOLD[entity]}
        frames[entity]=frame
    training=frames['late_delivery_training']
    count=training.count()
    if count!=30000 or count>config.max_training_rows:raise ValueError('Expected exactly 30000 bounded Gold training rows')
    scoring=frames['late_delivery_scoring']
    # Never collect open predictor rows, call a model on them, or write predictions.
    if scoring.filter(F.col('accepted_at').isNull()).limit(1).count():raise ValueError('Open acceptance is null')
    limits=scoring.agg(F.count(F.lit(1)).alias('rows'),F.date_format(F.min('accepted_at'),
                        "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias('first_open')).first()
    if limits['rows']!=600 or limits['first_open'] is None:raise ValueError('Expected exactly 600 Gold open orders')
    names=[n for n,_ in CONTRACTS_GOLD['late_delivery_training']]
    # Explicit bounded transfer. Encode timestamps IN Spark UTC before collect(), so
    # Python host timezone cannot change naive datetime values. This also avoids
    # private Spark-to-Pandas conversion dependencies in older runtime builds.
    timestamp_names={'accepted_at','promised_delivery_at_acceptance','outcome_available_at'}
    fields=[F.date_format(F.col(n),"yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias(n)
            if n in timestamp_names else F.col(n) for n in names]
    rows=training.select(*fields).limit(config.max_training_rows+1).collect()
    data=pd.DataFrame.from_records([row.asDict() for row in rows],columns=names)
    if len(data)!=count:raise ValueError('Gold training cardinality changed')
    for name in ('accepted_at','promised_delivery_at_acceptance','outcome_available_at'):
        data[name]=pd.to_datetime(data[name],utc=True,errors='raise')
    snapshots['source_contract']={'reference_date':config.gold.bronze.reference_date,
        'prefix':config.gold.bronze.prefix,'predictors':list(PREDICTORS),'open_rows':limits['rows'],
        'first_open_acceptance':limits['first_open'],'open_rows_scored':0}
    return data,limits['first_open'],snapshots
