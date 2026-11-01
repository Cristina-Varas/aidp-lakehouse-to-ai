"""Read-only evidence gate for an agent consuming the existing initial predictions."""
from datetime import datetime, timezone
import math
import re
from .agent_queries import AgentSnapshot, PREDICTIONS, SCORING, MODEL, RUN
from .gold import CONTRACTS_GOLD, V2_PREFIX
from .scoring import PREDICTION_CONTRACT


def _utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime):
        raise ValueError('Require a timestamp')
    # Spark collect() returns naive datetimes in the driver system timezone.
    # prepare_agent_data checks both Spark AND driver UTC before collecting.
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def validate_agent_records(predictions, scoring, table_ids, snapshot):
    """Pure validation; production gate accepts exactly the fixed 600-order cohort."""
    if not isinstance(snapshot, AgentSnapshot):
        raise ValueError('Require fixed Delta versions')
    if len(predictions) != 600 or len(scoring) != 600:
        raise ValueError('Require 600 predictions and 600 scoring rows')
    if set(table_ids) != {PREDICTIONS, SCORING} or any(not isinstance(v, str) or not v for v in table_ids.values()):
        raise ValueError('Require both Delta table identities')
    expected = {row['order_id']: row for row in scoring}
    keys = [row['order_id'] for row in predictions]
    if len(expected) != 600 or len(set(keys)) != 600 or set(keys) != set(expected):
        raise ValueError('Predictions must cover all unique scoring order keys exactly')
    provenance = set()
    alerts = 0
    for row in predictions:
        source = expected[row['order_id']]
        if any(v is None for v in row.values()) or any(v is None for v in source.values()):
            raise ValueError('Prediction and scoring rows must contain no null values')
        if (not re.fullmatch(r'O-[0-9]{8}', row['order_id'])
                or not re.fullmatch(r'C-[0-9]{6}', row['customer_id'])
                or row['customer_id'] != source['customer_id']
                or _utc(row['accepted_at']) != _utc(source['accepted_at'])):
            raise ValueError('Order/customer/acceptance reconciliation failed')
        if (row['model_name'] != MODEL or row['model_version'] != '1' or row['training_run_id'] != RUN
                or row['input_table'] != SCORING or row['input_table_id'] != table_ids[SCORING]
                or type(row['input_delta_version']) is not int
                or row['input_delta_version'] != snapshot.scoring_version
                or row['source_prefix'] != V2_PREFIX or row['_batch_id'] != 'initial'
                or str(row['_reference_date']) != '2026-10-28'
                or source['_batch_id'] != 'initial' or str(source['_reference_date']) != '2026-10-28'):
            raise ValueError('Unexpected model or initial Gold provenance')
        probability, threshold = row['late_probability'], row['decision_threshold']
        if (type(probability) not in (float, int) or type(threshold) not in (float, int)
                or not math.isfinite(probability) or not math.isfinite(threshold)
                or not 0 <= probability <= 1 or not 0 <= threshold <= 1
                or type(row['predicted_late']) is not bool or row['predicted_late'] != (probability >= threshold)):
            raise ValueError('Invalid probability or saved decision')
        if not re.fullmatch(r'[0-9a-f]{64}', row['input_fingerprint']):
            raise ValueError('Missing scoring input fingerprint')
        cutoff = _utc(row['training_cutoff'])
        if not cutoff <= _utc(row['accepted_at']) < _utc(source['promised_delivery_at_acceptance']):
            raise ValueError('Prediction cutoff/acceptance/promise is incompatible')
        if _utc(row['accepted_at']) > datetime(2026, 10, 28, tzinfo=timezone.utc):
            raise ValueError('Acceptance is later than the fixed observation time')
        provenance.add((threshold, cutoff.isoformat(), row['input_fingerprint']))
        alerts += row['predicted_late']
    if len(provenance) != 1:
        raise ValueError('Mixed decision thresholds, cutoffs or input fingerprints')
    threshold, cutoff, fingerprint = provenance.pop()
    return {'status': 'AGENT DATA READY', 'rows': 600, 'alerts': alerts, 'non_alerts': 600 - alerts,
            'model_name': MODEL, 'model_version': '1', 'training_run_id': RUN,
            'decision_threshold': threshold, 'training_cutoff': cutoff,
            'input_fingerprint': fingerprint, 'table_ids': dict(table_ids),
            'predictions_version': snapshot.predictions_version, 'scoring_version': snapshot.scoring_version,
            'observation_date': '2026-10-28', 'model_loaded': False, 'scoring_performed': False,
            'agent_deployed': False, 'native_sql_tool_verified': False, 'rag_verified': False}


def prepare_agent_data(spark, predictions_version=None):
    """Pin and reconcile data; no SQL DDL/DML, MLflow, training or inference."""
    import time
    from .aidp_bronze import _details
    from .gold_spark import _schema
    if spark.conf.get('spark.sql.session.timeZone') != 'UTC' or time.timezone != 0 or time.daylight:
        raise ValueError('Spark session and Python driver must both use UTC for timestamp collection')
    if predictions_version is not None and (type(predictions_version) is not int or predictions_version < 0):
        raise ValueError('Require a nonnegative predictions Delta version')
    table_ids = {}
    for table in (PREDICTIONS, SCORING):
        description = _details(spark.sql(f'DESCRIBE TABLE EXTENDED {table}').collect())
        if description.get('type', '').upper() != 'MANAGED' or description.get('provider', '').lower() != 'delta':
            raise ValueError('Agent input must remain managed Delta')
        table_ids[table] = spark.sql(f'DESCRIBE DETAIL {table}').first()['id']
    if predictions_version is None:
        predictions_version = int(spark.sql(f'DESCRIBE HISTORY {PREDICTIONS} LIMIT 1').first()['version'])
    predictions = spark.read.format('delta').option('versionAsOf', predictions_version).table(PREDICTIONS)
    _schema(predictions, PREDICTION_CONTRACT, PREDICTIONS)
    rows = [r.asDict(recursive=True) for r in predictions.orderBy('order_id').limit(601).collect()]
    if len(rows) != 600 or any(row['input_delta_version'] != rows[0]['input_delta_version'] for row in rows):
        raise ValueError('Require one complete scoring snapshot')
    snapshot = AgentSnapshot(predictions_version, rows[0]['input_delta_version'])
    source = spark.read.format('delta').option('versionAsOf', snapshot.scoring_version).table(SCORING)
    _schema(source, CONTRACTS_GOLD['late_delivery_scoring'], SCORING)
    source_rows = [r.asDict(recursive=True) for r in source.orderBy('order_id').limit(601).collect()]
    receipt = validate_agent_records(rows, source_rows, table_ids, snapshot)
    for table in table_ids:
        if spark.sql(f'DESCRIBE DETAIL {table}').first()['id'] != table_ids[table]:
            raise ValueError('Delta table identity changed during preparation')
    return receipt
