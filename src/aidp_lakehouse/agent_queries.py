"""Read-only, pinned Spark SQL for the operations adviser; no LLM-written SQL.

This module is also copied into the Custom Code ZIP, without project dependencies.
Only validated runtime values are interpolated. Connection IDs remain private
deployment configuration copied from this workspace's native SQL tool UI.
"""
from dataclasses import dataclass
import json
import re

PREDICTIONS = 'lakehouse_demo.gold.late_delivery_predictions'
SCORING = 'lakehouse_demo.gold.late_delivery_scoring'
MODEL = 'lakehouse_demo.gold.late_delivery_acceptance'
RUN = 'fc64485e353c41168cb90ee0f0bc5f71'
TOOLS = {
    'risk_summary': ('Counts, alert fraction and model provenance for all 600 open orders.', {}, 1),
    'rank_open_orders': ('Rank by estimated acceptance-time risk, not business priority. Maximum 20.', {'limit': 'number'}, 20),
    'get_order_risk': ('Look up an exact order ID, including score and acceptance attributes. No rows means not found in this snapshot.', {'order_id': 'string'}, 1),
    'get_customer_orders': ('Risk-ranked open orders for an exact customer ID, at most 20; total_matching states full count.', {'customer_id': 'string'}, 20),
    'risk_by_carrier': ('Counts, alerts and average estimated risk by planned carrier at acceptance; not actual delivery rates.', {}, 3),
    'risk_by_region': ('Counts, alerts and average estimated risk by original customer region; not actual delivery rates.', {}, 4),
}


@dataclass(frozen=True)
class AgentSnapshot:
    predictions_version: int
    scoring_version: int

    def __post_init__(self):
        for value in (self.predictions_version, self.scoring_version):
            if type(value) is not int or value < 0:
                raise ValueError('Require fixed nonnegative integer Delta versions')


def _parameters(tool, params):
    if tool not in TOOLS or not isinstance(params, dict):
        raise ValueError('Unknown query or invalid parameter object')
    expected = set(TOOLS[tool][1])
    if tool == 'rank_open_orders':
        if set(params) - expected:
            raise ValueError('Unexpected runtime parameter')
        limit = params.get('limit', 10)
        if type(limit) not in (int, float) or not 1 <= limit <= 20 or int(limit) != limit:
            raise ValueError('limit must be an integer from 1 to 20')
        return {'limit': int(limit)}
    if set(params) != expected:
        raise ValueError('Missing or unexpected runtime parameter')
    for name, pattern in (('order_id', r'O-[0-9]{8}'), ('customer_id', r'C-[0-9]{6}')):
        if name in params and (not isinstance(params[name], str) or not re.fullmatch(pattern, params[name])):
            raise ValueError('Use an exact synthetic ' + name)
    return params


def query_sql(tool, params, snapshot):
    """Validated constants only; no native {{parameter}} expansion is used."""
    values = _parameters(tool, params)
    if not isinstance(snapshot, AgentSnapshot):
        raise ValueError('Require an explicit AgentSnapshot')
    context = f'''WITH context AS (
SELECT p.order_id, p.customer_id, p.late_probability, p.predicted_late,
       p.decision_threshold, p.model_name, p.model_version, p.training_run_id,
       p.training_cutoff, p.input_delta_version, p.input_fingerprint, p._reference_date,
       s.accepted_at, s.promised_delivery_at_acceptance,
       s.priority_at_acceptance, s.planned_carrier_at_acceptance,
       s.customer_region_at_acceptance, s.customer_segment_at_acceptance,
       s.inventory_fill_rate_at_acceptance, s.warehouse_load_at_acceptance
FROM {PREDICTIONS} VERSION AS OF {snapshot.predictions_version} p
JOIN {SCORING} VERSION AS OF {snapshot.scoring_version} s
  ON p.order_id = s.order_id AND p.customer_id = s.customer_id
)'''
    evidence = '''order_id, customer_id, late_probability, predicted_late, decision_threshold,
       model_name, model_version, training_run_id, training_cutoff, input_delta_version,
       accepted_at, promised_delivery_at_acceptance, priority_at_acceptance,
       planned_carrier_at_acceptance, customer_region_at_acceptance,
       customer_segment_at_acceptance, inventory_fill_rate_at_acceptance,
       warehouse_load_at_acceptance, _reference_date'''
    if tool == 'risk_summary':
        body = '''SELECT COUNT(*) AS open_orders,
       SUM(CASE WHEN predicted_late THEN 1 ELSE 0 END) AS alert_orders,
       SUM(CASE WHEN NOT predicted_late THEN 1 ELSE 0 END) AS non_alert_orders,
       AVG(CASE WHEN predicted_late THEN 1.0 ELSE 0.0 END) AS alert_fraction,
       MIN(late_probability) AS min_score, MAX(late_probability) AS max_score,
       MIN(decision_threshold) AS decision_threshold, MIN(model_name) AS model_name,
       MIN(model_version) AS model_version, MIN(training_run_id) AS training_run_id,
       MIN(training_cutoff) AS training_cutoff, MIN(_reference_date) AS observation_date
FROM context'''
    elif tool == 'rank_open_orders':
        body = f'''SELECT ROW_NUMBER() OVER (ORDER BY late_probability DESC, order_id ASC) AS risk_rank,
       {evidence}
FROM context ORDER BY late_probability DESC, order_id ASC LIMIT {values['limit']}'''
    elif tool == 'get_order_risk':
        body = f"SELECT {evidence} FROM context WHERE order_id = '{values['order_id']}' LIMIT 1"
    elif tool == 'get_customer_orders':
        body = f'''SELECT COUNT(*) OVER () AS total_matching, {evidence}
FROM context WHERE customer_id = '{values['customer_id']}'
ORDER BY late_probability DESC, order_id ASC LIMIT 20'''
    else:
        group = 'planned_carrier_at_acceptance' if tool == 'risk_by_carrier' else 'customer_region_at_acceptance'
        body = f'''SELECT {group}, COUNT(*) AS open_orders,
       SUM(CASE WHEN predicted_late THEN 1 ELSE 0 END) AS alert_orders,
       AVG(CASE WHEN predicted_late THEN 1.0 ELSE 0.0 END) AS alert_fraction,
       AVG(late_probability) AS average_estimated_risk
FROM context GROUP BY {group} ORDER BY average_estimated_risk DESC, {group} ASC'''
    return context + '\n' + body


def native_sql_config(connection, tool, params, snapshot):
    """Keep verified workspace routing; generate a static query after validation.

    The caller supplies the exact native Spark SQL configuration exported from
    the UI, including its dialect/cluster fields. Those keys are not guessed.
    No credentials belong in this dictionary or in generated repository assets.
    """
    if isinstance(connection, str):
        # AIDP's Custom Code Parameters editor serializes nested configuration
        # objects to JSON text. Decode only JSON, retaining all target checks.
        def unique_keys(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('Duplicate SQL configuration key')
                result[key] = value
            return result
        try:
            connection = json.loads(connection, object_pairs_hook=unique_keys)
        except (TypeError, ValueError) as exc:
            raise ValueError('SQL connection must be an object or valid JSON object text') from exc
    if not isinstance(connection, dict) or not connection:
        raise ValueError('Supply a verified Spark SQL tool configuration from this workspace')
    if connection.get('catalogKey') != 'lakehouse_demo' or connection.get('schemaKey') != 'gold':
        raise ValueError('SQL connection must target lakehouse_demo.gold')
    if any(k in connection for k in ('password', 'token', 'secret', 'api_key')):
        raise ValueError('Use native connection references, not credentials')
    conf = dict(connection)
    if conf.get('queryType') == 'SPARK':
        # Observed native SQLTool routes by catalogType, while the exported
        # canvas configuration uses queryType. Without this translation the
        # toolkit defaults to Oracle EXTERNAL validation, rejecting Spark SQL.
        for key in ('catalogType', 'catalog_type'):
            if key in conf and str(conf[key]).strip().upper() != 'STANDARD':
                raise ValueError('Spark SQL configuration conflicts with catalog type')
        conf['catalogType'] = 'STANDARD'
        cluster = conf.get('sparkComputeKey')
        if not isinstance(cluster, str) or not cluster.strip():
            raise ValueError('Spark SQL configuration requires its verified compute key')
        if 'clusterKey' in conf and conf['clusterKey'] != cluster:
            raise ValueError('Spark SQL configuration has conflicting compute keys')
        # The inspected native _SparkSQLExecutor reads clusterKey, not the
        # canvas export's sparkComputeKey. Preserve the exported key as well.
        conf['clusterKey'] = cluster
    conf.update(query=query_sql(tool, params, snapshot), isRowLimitEnabled=True, maxRows=TOOLS[tool][2])
    return conf
