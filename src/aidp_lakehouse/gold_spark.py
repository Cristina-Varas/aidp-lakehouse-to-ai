"""Distributed Spark Gold joins/aggregations and guarded insert-only Delta publishing.

Only bounded aggregate summaries/metadata reach the driver. No business-row
collection, Pandas, UDF, SELECT *, source mutations or cloud SDK is used.
"""
from functools import reduce
from .aidp_bronze import (ENTITIES, _details, identifier, require_standard_catalog, read_control_documents)
from .silver import KEYS as SILVER_KEYS
from .silver_spark import output_contract
from .gold import (GoldConfig, CONTRACTS_GOLD, KEYS, PREDICTORS, IDENTIFIERS, EXPECTED_SILVER,
                   ORDER_RENAMES, DELIVERY_RENAMES, NULLABLE_OPERATIONS)


def _fail_if(frame, condition, message):
    if frame.filter(condition).limit(1).count():
        raise ValueError(message)


def _schema(frame, contract, name):
    actual = [(f.name,f.dataType.simpleString()) for f in frame.schema.fields]
    if actual != contract:
        raise ValueError(f'Incompatible {name} schema; preserve existing tables')


def _unique(frame, keys, name):
    from pyspark.sql import functions as F
    _fail_if(frame, reduce(lambda a,b:a|b,(F.col(k).isNull() for k in keys)), f'{name}: null key')
    _fail_if(frame.groupBy(*keys).count(),F.col('count')!=1,f'{name}: duplicate key')


def _same(left, right, name):
    # Gold has no MAP columns; arrays are canonically sorted during aggregation.
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise ValueError(f'{name}: full-row multiset results differ')


def _anti(child, parent, key, name):
    if child.select(key).join(parent.select(key),key,'left_anti').limit(1).count():
        raise ValueError(f'{name}: unknown {key}')


def validate_inputs(frames, *, expected_counts=None, cutoff='2026-10-28T00:00:00Z'):
    """Validate Silver contracts and lifecycle/cardinality before constructing Gold."""
    from pyspark.sql import functions as F
    counts = {}
    for entity in ENTITIES:
        frame=frames[entity]
        _schema(frame,output_contract(entity),f'Silver {entity}')
        _unique(frame,(SILVER_KEYS[entity],),f'Silver {entity}')
        _fail_if(frame,reduce(lambda a,b:a|b,(F.col(n).isNull() for n,_ in output_contract(entity))),
                 f'Silver {entity}: unexpected null')
        _fail_if(frame,(F.col('source_updated_at')>F.to_timestamp(F.lit(cutoff))) |
                 (F.col('source_created_at')>F.col('source_updated_at')), f'Silver {entity}: invalid source chronology')
        counts[entity]=frame.count()
        if expected_counts is not None and counts[entity]!=expected_counts[entity]:
            raise ValueError(f'Silver {entity}: unexpected count {counts[entity]}')
    customers,orders,deliveries,events=(frames[e] for e in ENTITIES)
    _unique(deliveries,('order_id',),'Silver deliveries/order')
    _anti(orders,customers,'customer_id','orders');_anti(deliveries,orders,'order_id','deliveries')
    _anti(events,orders,'order_id','events')
    lifecycle=orders.select('order_id','accepted_at','promised_delivery_at_acceptance','current_status').join(
        deliveries.select('order_id','dispatched_at','delivered_at'), 'order_id','left')
    _fail_if(lifecycle,~F.col('current_status').isin('OPEN','DELIVERED') |
             (F.col('promised_delivery_at_acceptance')<=F.col('accepted_at')) |
             ((F.col('current_status')=='DELIVERED') & F.col('delivered_at').isNull()) |
             ((F.col('current_status')=='OPEN') & F.col('delivered_at').isNotNull()) |
             (F.col('accepted_at')>F.to_timestamp(F.lit(cutoff))) |
             (F.col('dispatched_at')<F.col('accepted_at')) |
             (F.col('delivered_at')<F.col('dispatched_at')),
             'Silver order/delivery lifecycle inconsistent')
    _fail_if(events,~F.col('event_type').isin('ACCEPTED','DISPATCHED','DELIVERED','INCIDENT_OPENED'),
             'Silver events: unexpected type')
    ev=events.join(lifecycle,'order_id')
    _fail_if(ev,(F.col('event_at')<F.col('accepted_at')) |
             (F.col('event_at')>F.col('source_updated_at')) |
             ((F.col('event_type')=='ACCEPTED') & (F.col('event_at')!=F.col('accepted_at'))) |
             ((F.col('event_type')!='ACCEPTED') & F.col('delivered_at').isNull()) |
             (F.col('event_at')>F.col('delivered_at')) |
             ((F.col('event_type')=='DISPATCHED') & (F.col('event_at')!=F.col('dispatched_at'))) |
             ((F.col('event_type')=='DELIVERED') & (F.col('event_at')!=F.col('delivered_at'))) |
             ((F.col('event_type')=='INCIDENT_OPENED') &
              (~F.col('payload')['exception_type'].isin('CAPACITY','STOCK','TRANSPORT') |
               F.col('payload')['exception_type'].isNull())), 'Silver events: lifecycle mismatch')
    stages=events.groupBy('order_id').agg(*[
        F.sum(F.when(F.col('event_type')==t,1).otherwise(0)).alias(t.lower()+'_events')
        for t in ('ACCEPTED','DISPATCHED','DELIVERED')]).join(lifecycle,'order_id','right')
    _fail_if(stages,F.col('accepted_events').isNull() | (F.col('accepted_events')!=1) |
             ((F.col('current_status')=='DELIVERED') &
              ((F.col('dispatched_events')!=1)|(F.col('delivered_events')!=1))),
             'Silver events: missing/duplicate lifecycle stages')
    return counts


def transform_frames(frames, *, reference_date='2026-10-28'):
    """Distributed transformations only; caller validates the immutable input snapshot."""
    from pyspark.sql import functions as F
    if reference_date!='2026-10-28':raise ValueError('Reference date must remain 2026-10-28')
    orders,customers,deliveries,events=(frames[e] for e in ('orders','customers','deliveries','events'))
    if orders.sparkSession.conf.get('spark.sql.session.timeZone')!='UTC':
        raise ValueError('Gold requires spark.sql.session.timeZone=UTC')
    event_summary=events.groupBy('order_id').agg(
        F.count(F.lit(1)).alias('event_count'),
        F.sum(F.when(F.col('event_type')=='INCIDENT_OPENED',1).otherwise(0)).alias('incident_count'),
        F.sort_array(F.collect_set(F.when(F.col('event_type')=='INCIDENT_OPENED',
                                        F.col('payload')['exception_type']))).alias('incident_types'),
        F.min(F.when(F.col('event_type')=='INCIDENT_OPENED',F.col('event_at'))).alias('first_incident_at'),
        F.max(F.when(F.col('event_type')=='INCIDENT_OPENED',F.col('event_at'))).alias('last_incident_at'),
        F.max('event_at').alias('last_event_at'),
        F.max(F.when(F.col('event_type')=='DELIVERED',F.col('source_updated_at'))).alias('_delivered_event_updated_at'))
    order_fields=[F.col(n).alias(ORDER_RENAMES.get(n,n)) for n,_ in output_contract('orders') if not n.startswith('_')]
    o=orders.select(*order_fields,F.col('_source_file').alias('order_source_file'))
    c=customers.select('customer_id','customer_name',F.col('region').alias('customer_region_current'),
                       F.col('segment').alias('customer_segment_current'))
    d=deliveries.select(*[F.col(n).alias(DELIVERY_RENAMES.get(n,n)) for n,_ in output_contract('deliveries')
                          if not n.startswith('_')],F.col('_source_file').alias('delivery_source_file'))
    # Aggregate events BEFORE joining: one row per order throughout these joins.
    joined=o.join(c,'customer_id','left').join(d,'order_id','left').join(event_summary,'order_id','left')
    operations=(joined.withColumn('is_late',F.when(F.col('delivered_at').isNotNull(),
                       F.col('delivered_at')>F.col('promised_delivery_at_acceptance')))
        .withColumn('outcome_available_at',F.when(F.col('delivered_at').isNotNull(),
                    F.greatest('delivered_at','delivery_source_updated_at','_delivered_event_updated_at')))
        .withColumn('_batch_id',F.lit('initial')).withColumn('_reference_date',F.to_date(F.lit(reference_date))))
    operations=operations.select(*[n for n,_ in CONTRACTS_GOLD['order_operations']])
    completed=operations.filter(F.col('delivered_at').isNotNull())
    kpis=(completed.withColumn('period_start',F.to_date(F.date_trunc('month','delivered_at')))
        .groupBy('period_start','customer_region_at_acceptance','actual_carrier')
        .agg(F.count(F.lit(1)).alias('deliveries'),F.sum(F.col('is_late').cast('bigint')).alias('late_deliveries'))
        .withColumn('late_delivery_pct',(F.lit(100)*F.col('late_deliveries')/F.col('deliveries')).cast('decimal(9,6)'))
        .withColumn('_batch_id',F.lit('initial')).withColumn('_reference_date',F.to_date(F.lit(reference_date))))
    # No wildcard or schema-derived model columns: the explicit roles are the contract.
    training=completed.select(*IDENTIFIERS,*PREDICTORS,'is_late','outcome_available_at','_batch_id','_reference_date')
    scoring=operations.filter(F.col('delivered_at').isNull()).select(*IDENTIFIERS,*PREDICTORS,'_batch_id','_reference_date')
    return {'order_operations':operations,'delivery_kpis':kpis,
            'late_delivery_training':training,'late_delivery_scoring':scoring}


def validate_outputs(inputs, outputs):
    from pyspark.sql import functions as F
    counts={}
    for name,frame in outputs.items():
        _schema(frame,CONTRACTS_GOLD[name],name);_unique(frame,KEYS[name],name)
        required=[n for n,_ in CONTRACTS_GOLD[name] if name!='order_operations' or n not in NULLABLE_OPERATIONS]
        _fail_if(frame,reduce(lambda a,b:a|b,(F.col(n).isNull() for n in required)),f'{name}: unexpected null')
        _fail_if(frame,(F.col('_batch_id')!='initial') |
                 (F.col('_reference_date')!=F.to_date(F.lit('2026-10-28'))),f'{name}: snapshot metadata differs')
        counts[name]=frame.count()
    ops,train,score,kpis=(outputs[n] for n in ('order_operations','late_delivery_training','late_delivery_scoring','delivery_kpis'))
    if counts['order_operations']!=inputs['orders'].count() or counts['late_delivery_training']!=inputs['deliveries'].count():
        raise ValueError('Gold/order/delivery cardinalities differ')
    if counts['late_delivery_training']+counts['late_delivery_scoring']!=counts['order_operations']:
        raise ValueError('Training/scoring partition does not reconcile')
    _same(ops.select('order_id'),inputs['orders'].select('order_id'),'operational order keys')
    _same(ops.select(*IDENTIFIERS,*PREDICTORS),inputs['orders'].select(*IDENTIFIERS,*PREDICTORS),
          'original acceptance attributes')
    delivery_names=[n for n,_ in output_contract('deliveries') if not n.startswith('_')]
    _same(ops.filter(F.col('delivered_at').isNotNull()).select(*[
        F.col(DELIVERY_RENAMES.get(n,n)).alias(n) for n in delivery_names]),
        inputs['deliveries'].select(*delivery_names),'original delivery outcomes')
    _same(train.select('order_id'),inputs['deliveries'].select('order_id'),'training delivered keys')
    _same(score.select('order_id'),inputs['orders'].filter(F.col('current_status')=='OPEN').select('order_id'),'scoring open keys')
    is_open=F.col('current_status')=='OPEN'
    nullable_delivery=[n for n in NULLABLE_OPERATIONS if n not in ('first_incident_at','last_incident_at')]
    _fail_if(ops,(is_open & reduce(lambda a,b:a|b,(F.col(n).isNotNull() for n in nullable_delivery))) |
             (~is_open & reduce(lambda a,b:a|b,(F.col(n).isNull() for n in nullable_delivery))) |
             ((F.col('delivered_at').isNotNull()) &
              (F.col('is_late')!=(F.col('delivered_at')>F.col('promised_delivery_at_acceptance')))) |
             (F.col('outcome_available_at')<F.col('delivered_at')) |
             (F.col('outcome_available_at')<F.col('accepted_at')) |
             (F.col('outcome_available_at')>F.to_timestamp(F.lit('2026-10-28T00:00:00Z'))) |
             ((F.col('incident_count')==0) &
              (F.col('first_incident_at').isNotNull()|F.col('last_incident_at').isNotNull())) |
             ((F.col('incident_count')>0) &
              (F.col('first_incident_at').isNull()|F.col('last_incident_at').isNull())),
             'Operational label/nullability/outcome chronology mismatch')
    _same(train,ops.filter(~is_open).select(*[n for n,_ in CONTRACTS_GOLD['late_delivery_training']]),'training projection')
    _same(score,ops.filter(is_open).select(*[n for n,_ in CONTRACTS_GOLD['late_delivery_scoring']]),'scoring projection')
    expected=transform_frames(inputs)['delivery_kpis']
    _same(kpis,expected,'KPI dimensional reconciliation')
    totals=kpis.agg(F.coalesce(F.sum('deliveries'),F.lit(0)).alias('deliveries'),
                    F.coalesce(F.sum('late_deliveries'),F.lit(0)).alias('late')).first()
    late=train.filter(F.col('is_late')).count()
    if totals['deliveries']!=counts['late_delivery_training'] or totals['late']!=late:
        raise ValueError('KPI denominator/late label totals differ')
    _fail_if(kpis,(F.col('deliveries')<=0)|(F.col('late_deliveries')<0)|
             (F.col('late_deliveries')>F.col('deliveries'))|
             (F.col('late_delivery_pct')<0)|(F.col('late_delivery_pct')>100),'Invalid KPI denominator/rate')
    events=ops.agg(F.sum('event_count').alias('events'),F.sum('incident_count').alias('incidents')).first()
    if events['events']!=inputs['events'].count() or events['incidents']!=inputs['events'].filter(F.col('event_type')=='INCIDENT_OPENED').count():
        raise ValueError('Event aggregation multiplied or lost records')
    return {'row_counts':counts,'late_deliveries':late,'delivered_orders':totals['deliveries'],
            'open_orders':counts['late_delivery_scoring'],'reconciliation':'PASS'}


def fingerprints(outputs):
    """Order-independent diagnostic digest; full-row equality remains authoritative."""
    from pyspark.sql import functions as F
    result={}
    for name,frame in outputs.items():
        digest=F.sha2(F.to_json(F.struct(*[F.col(n) for n,_ in CONTRACTS_GOLD[name]]),
                               options={'ignoreNullFields':'false'}),256)
        hashes=frame.select(digest.alias('h'))
        # Sum four 64-bit limbs as exact decimals (38 digits), independent of partitions.
        row=hashes.agg(F.count(F.lit(1)).alias('rows'),*[F.coalesce(F.sum(
            F.conv(F.substring('h',1+16*i,16),16,10).cast('decimal(38,0)')),F.lit(0)).alias(f'limb{i}')
            for i in range(4)]).first()
        result[name]={k:str(v) if k!='rows' else v for k,v in row.asDict().items()}
    return result


def _preflight(existing, planned, name):
    _schema(existing,CONTRACTS_GOLD[name],name);_unique(existing,KEYS[name],name)
    if existing.exceptAll(planned).limit(1).count():
        raise ValueError(f'Existing {name} contains changed or extra rows; preserved')


def publish_initial(spark, outputs, namespace, *, forbidden_source_root):
    """All-target preflight, then insert-only Delta MERGE, never matched-row mutation."""
    from delta.tables import DeltaTable
    for part in namespace.split('.'):identifier(part)
    if set(outputs)!=set(CONTRACTS_GOLD):raise ValueError('Gold must contain exactly four contracted outputs')
    visible={r.asDict()['tableName'] for r in spark.sql(f'SHOW TABLES IN {namespace}').collect()}
    before={}
    for entity,planned in outputs.items():
        _schema(planned,CONTRACTS_GOLD[entity],entity);_unique(planned,KEYS[entity],entity)
        before[entity]=0
        if entity not in visible:continue
        name=f'{namespace}.{entity}'
        detail=_details(spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect())
        if detail.get('type','').upper()!='MANAGED' or detail.get('provider','').lower()!='delta':
            raise ValueError(f'{name} is not managed Delta; preserved')
        location=detail.get('location','').rstrip('/')
        if not location or location==forbidden_source_root.rstrip('/') or location.startswith(forbidden_source_root.rstrip('/')+'/'):
            raise ValueError('Gold managed target overlaps source data or has no confirmed location')
        target=spark.table(name);_preflight(target,planned,entity);before[entity]=target.count()
    summary={}
    for entity,planned in outputs.items():
        name=f'{namespace}.{entity}'
        if entity not in visible:
            fields=', '.join(f'{n} {t.upper()}' for n,t in CONTRACTS_GOLD[entity])
            spark.sql(f'CREATE TABLE IF NOT EXISTS {name} ({fields}) USING DELTA')
        detail=_details(spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect())
        if detail.get('type','').upper()!='MANAGED' or detail.get('provider','').lower()!='delta':
            raise ValueError(f'{name} changed to an incompatible definition before MERGE')
        location=detail.get('location','').rstrip('/')
        if not location or location==forbidden_source_root.rstrip('/') or location.startswith(forbidden_source_root.rstrip('/')+'/'):
            raise ValueError('Gold managed target overlaps protected source data')
        _schema(spark.table(name),CONTRACTS_GOLD[entity],entity)
        predicate=' AND '.join(f't.{k} = s.{k}' for k in KEYS[entity])
        DeltaTable.forName(spark,name).alias('t').merge(planned.alias('s'),predicate).whenNotMatchedInsertAll().execute()
        actual=spark.table(name);_same(actual,planned,f'Published {entity}')
        count=actual.count()
        summary[entity]={'resulting_rows':count,'inserted_rows':count-before[entity],
                         'data_types':dict(CONTRACTS_GOLD[entity])}
    return summary


def read_silver(spark, config):
    cfg=config.bronze
    require_standard_catalog(spark,cfg)
    manifest,_=read_control_documents(spark,cfg)
    expected={e:manifest['row_counts'][cfg.file_uri(e).rsplit('/',1)[1]] for e in ENTITIES}
    if expected!=EXPECTED_SILVER:raise ValueError('Initial Gold manifest counts differ from the verified dataset')
    inputs={}
    # Capture immutable Delta versions; later table commits cannot change this run.
    for entity in (*ENTITIES,'quarantine'):
        name=f'{config.silver_namespace}.{entity}'
        detail=_details(spark.sql(f'DESCRIBE TABLE EXTENDED {name}').collect())
        if detail.get('type','').upper()!='MANAGED' or detail.get('provider','').lower()!='delta':
            raise ValueError(f'{name} must be managed Delta')
        version=spark.sql(f'DESCRIBE HISTORY {name} LIMIT 1').first()['version']
        frame=spark.read.format('delta').option('versionAsOf',version).table(name)
        _schema(frame,output_contract(entity),f'Silver {entity}')
        if entity=='quarantine':
            if frame.limit(1).count():raise ValueError('Initial Silver quarantine is not empty')
            continue
        from pyspark.sql import functions as F
        _fail_if(frame,(F.col('_batch_id')!=cfg.batch_id)|(F.col('_source_file')!=cfg.file_uri(entity)),
                 f'Silver {entity} is not the exact initial v2 source')
        inputs[entity]=frame
    return inputs,expected


def build_initial_gold(spark, config:GoldConfig):
    """Run once; notebook repeats this entry point to prove source/result stability."""
    spark.conf.set('spark.sql.session.timeZone','UTC')
    inputs={}
    outputs={}
    try:
        source_frames,expected=read_silver(spark,config)
        inputs={n:f.cache() for n,f in source_frames.items()}
        validate_inputs(inputs,expected_counts=expected)
        outputs={n:f.cache() for n,f in transform_frames(inputs,reference_date=config.bronze.reference_date).items()}
        validation=validate_outputs(inputs,outputs)
        digest=fingerprints(outputs)
        published=publish_initial(spark,outputs,config.gold_namespace,forbidden_source_root=config.bronze.root_uri.split('/source-data/')[0]+'/source-data')
        return {'validation':validation,'fingerprints':digest,'publication':published}
    finally:
        for frame in (*outputs.values(),*inputs.values()):frame.unpersist()
