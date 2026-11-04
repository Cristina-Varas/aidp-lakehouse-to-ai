"""Order-risk response roles derived from the saved training feature artifact.

No model loading, training, scoring, attribution or cloud mutation occurs here.
The caller downloads the exact registered run's artifact using native auth.
"""
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math

from .agent_queries import MODEL, RUN
from .agent_native_sql import native_parameter_query
from .training_contract import feature_contract
from .training_contract import instant

SCHEMA = 'order-risk-response/1'
PREDICTION_FIELDS = ('late_probability', 'predicted_late', 'decision_threshold')


@dataclass(frozen=True)
class TrainingFeatureEvidence:
    artifact_bytes: bytes
    model_name: str
    model_version: str
    training_run_id: str

    def __post_init__(self):
        if (self.model_name != MODEL or self.model_version != '1' or self.training_run_id != RUN):
            raise ValueError('Require the verified exact model version and training run')
        if not isinstance(self.artifact_bytes, bytes):
            raise ValueError('Supply the downloaded feature_contract.json bytes')
        if self.contract != feature_contract():
            raise ValueError('Saved training contract differs from the supported acceptance pipeline')

    @property
    def contract(self):
        return json.loads(self.artifact_bytes)

    @property
    def sha256(self):
        return hashlib.sha256(self.artifact_bytes).hexdigest()

    @classmethod
    def from_native_records(cls, raw, model_version, run_info):
        if run_info.get('status') != 'FINISHED' or run_info.get('run_id') != model_version.get('run_id'):
            raise ValueError('Require a finished run matching the exact registered version')
        return cls(raw, model_version.get('name'), str(model_version.get('version')), model_version.get('run_id'))


def _literal(text):
    return "'" + text.replace("'", "''") + "'"


def _struct(items):
    return 'NAMED_STRUCT(' + ', '.join(_literal(k) + ', ' + v for k, v in items) + ')'


def order_risk_response_query(snapshot, evidence):
    """One structured response per exact order; keep the pinned key guard.

    Raw UTC dates are derivation inputs. Only promised_lead_days enters the
    fitted numeric preprocessing; model outputs never appear in input values.
    """
    if not isinstance(evidence, TrainingFeatureEvidence):
        raise ValueError('Require verified saved training feature evidence')
    contract = evidence.contract
    original_numeric = [n for n in contract['numeric'] if n not in contract['derivation']]
    categorical = contract['categorical']
    base = native_parameter_query('get_order_risk', snapshot)
    # Extend this lookup alone with the three acceptance attributes omitted by
    # the former flat evidence projection. Other five native queries stay intact.
    anchor = 's.inventory_fill_rate_at_acceptance, s.warehouse_load_at_acceptance'
    projection = 'FROM context WHERE order_id ='
    if base.count(anchor) != 1 or base.count(projection) != 1:
        raise ValueError('Unexpected lookup source; refuse an unreviewed projection')
    extra = ('transit_distance_km', 'line_count', 'units')
    base = base.replace(anchor, anchor + ', ' + ', '.join('s.' + n for n in extra), 1)
    base = base.replace(projection, ', ' + ', '.join(extra) + '\n' + projection, 1)
    prediction = _struct([(n, n) for n in PREDICTION_FIELDS] + [
        ('role', _literal('Prediction and threshold-based decision; these fields are NOT predictors.'))])
    values = [(n, n) for n in [*categorical, *original_numeric]]
    values.append(('promised_lead_days',
                   'CAST(UNIX_MICROS(promised_delivery_at_acceptance) - UNIX_MICROS(accepted_at) AS DOUBLE) / CAST(86400000000 AS DOUBLE)'))
    inputs = _struct([('feature_values', _struct(values)),
        ('categorical_feature_names', 'ARRAY(' + ', '.join(map(_literal, categorical)) + ')'),
        ('numeric_feature_names', 'ARRAY(' + ', '.join(map(_literal, contract['numeric'])) + ')'),
        ('role', _literal('Only these named feature values enter fitted preprocessing; source is the saved training feature contract.'))])
    dates = _struct([('accepted_at', 'accepted_at'),
                    ('promised_delivery_at_acceptance', 'promised_delivery_at_acceptance'),
                    ('usage', _literal(contract['raw_timestamps'])),
                    ('derivation', _literal(contract['derivation']['promised_lead_days'])),
                    ('separate_fitted_features', 'FALSE')])
    metadata = _struct([(n, n) for n in ('order_id', 'customer_id', 'model_name', 'model_version',
        'training_run_id', 'training_cutoff', 'input_delta_version', '_reference_date')] + [
        ('prediction_delta_version', str(snapshot.predictions_version)),
        ('scoring_delta_version', str(snapshot.scoring_version)),
        ('feature_contract_artifact', _literal('feature_contract.json')),
        ('feature_contract_sha256', _literal(evidence.sha256)),
        ('role', _literal('Identifiers and provenance only; NOT predictors.'))])
    attribution = _struct([('individual_feature_attribution_available', 'FALSE'),
                          ('causal_explanation_available', 'FALSE'),
                          ('statement', _literal('No validated per-order attribution or causal explanation is available. Do not infer which variable caused this score.'))])
    response = _struct([('schema_version', _literal(SCHEMA)), ('prediction', prediction),
        ('model_inputs', inputs), ('derivation_source_dates', dates), ('metadata', metadata),
        ('attribution', attribution)])
    return f'WITH matched_order AS (\n{base}\n)\nSELECT TO_JSON({response}) AS order_risk_response\nFROM matched_order'


def apply_order_risk_contract(tools, snapshot, evidence):
    """Modify only the lookup's response query/description; preserve all routing."""
    result = deepcopy(tools)
    matches = [t for t in result if t.get('displayName') == 'get_order_risk']
    if len(matches) != 1 or matches[0].get('toolType') != 'SQL':
        raise ValueError('Require exactly one existing native SQL order lookup')
    tool = matches[0]
    tool['toolConfig']['query'] = order_risk_response_query(snapshot, evidence)
    tool['description'] = ('Exact order lookup with a role-separated JSON response. prediction contains '
        'late_probability, predicted_late and decision_threshold: outputs/decision rule, NEVER predictors. '
        'model_inputs.feature_values lists the saved training inputs. derivation_source_dates are used '
        'only to derive promised_lead_days. metadata holds identifiers/provenance. attribution is unavailable; '
        'do not infer individual or causal contributions. Zero rows means absent from this snapshot.')
    return result


def validate_order_risk_response(payload, snapshot, evidence):
    """Check a visible native result, including the exact feature roles/units."""
    if isinstance(payload, str): payload = json.loads(payload)
    expected = {'schema_version', 'prediction', 'model_inputs', 'derivation_source_dates',
                'metadata', 'attribution'}
    if set(payload) != expected or payload['schema_version'] != SCHEMA:
        raise ValueError('Unexpected order-risk response schema')
    contract = evidence.contract
    values = payload['model_inputs']['feature_values']
    if (set(values) != set(contract['categorical'] + contract['numeric'])
            or payload['model_inputs']['categorical_feature_names'] != contract['categorical']
            or payload['model_inputs']['numeric_feature_names'] != contract['numeric']):
        raise ValueError('Response predictors differ from the saved training contract')
    if set(PREDICTION_FIELDS) & set(values):
        raise ValueError('Prediction outputs must never be model inputs')
    if any(not isinstance(values[n], str) or not values[n] for n in contract['categorical']):
        raise ValueError('Missing categorical input values')
    if any(type(values[n]) not in (int, float) or not math.isfinite(values[n]) for n in contract['numeric']):
        raise ValueError('Require finite numeric feature values')
    dates = payload['derivation_source_dates']
    days = (instant(dates['promised_delivery_at_acceptance']) - instant(dates['accepted_at'])).total_seconds() / 86400
    if dates.get('separate_fitted_features') is not False or not math.isclose(values['promised_lead_days'], days, rel_tol=0, abs_tol=1e-12):
        raise ValueError('Original dates must derive the promised lead time consistently')
    meta = payload['metadata']
    for key, expected_value in {'model_name': evidence.model_name, 'model_version': evidence.model_version,
                               'training_run_id': evidence.training_run_id,
                               'prediction_delta_version': snapshot.predictions_version,
                               'scoring_delta_version': snapshot.scoring_version,
                               'input_delta_version': snapshot.scoring_version,
                               'feature_contract_sha256': evidence.sha256,
                               'feature_contract_artifact': 'feature_contract.json'}.items():
        if meta.get(key) != expected_value: raise ValueError('Response provenance differs: ' + key)
    prediction = payload['prediction']
    probability, threshold = prediction['late_probability'], prediction['decision_threshold']
    if (type(probability) not in (int, float) or not math.isfinite(probability) or not 0 <= probability <= 1
            or type(threshold) not in (int, float) or not math.isfinite(threshold) or not 0 <= threshold <= 1
            or type(prediction['predicted_late']) is not bool
            or prediction['predicted_late'] != (probability >= threshold)):
        raise ValueError('Invalid prediction or decision consistency')
    attribution = payload['attribution']
    if (attribution.get('individual_feature_attribution_available') is not False
            or attribution.get('causal_explanation_available') is not False):
        raise ValueError('No validated individual or causal attribution is available')
    return {'status': 'ORDER RISK CONTRACT VERIFIED', 'feature_names': sorted(values),
            'feature_contract_sha256': evidence.sha256, 'promised_lead_days': days}
