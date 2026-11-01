"""Bounded initial inference using a verified, saved acceptance-time pipeline."""
from dataclasses import dataclass, field
import hashlib
import math
import re
from .gold import GoldConfig, CONTRACTS_GOLD, PREDICTORS
from .training_contract import feature_contract, instant

VERSION = '0.6.0'
MODEL_NAME = 'lakehouse_demo.gold.late_delivery_acceptance'
MODEL_VERSION = '1'
TRAINING_RUN = 'fc64485e353c41168cb90ee0f0bc5f71'
PREDICTION_CONTRACT = [
    ('order_id','string'), ('customer_id','string'), ('accepted_at','timestamp'),
    ('late_probability','double'), ('predicted_late','boolean'), ('decision_threshold','double'),
    ('model_name','string'), ('model_version','string'), ('training_run_id','string'),
    ('training_cutoff','timestamp'), ('input_table','string'), ('input_table_id','string'),
    ('input_delta_version','bigint'), ('input_fingerprint','string'),
    ('source_prefix','string'), ('_batch_id','string'), ('_reference_date','date'),
]


@dataclass(frozen=True)
class ScoringConfig:
    gold: GoldConfig = field(default_factory=GoldConfig)
    registered_model_name: str = MODEL_NAME
    model_version: str = MODEL_VERSION
    training_run_id: str = TRAINING_RUN
    input_delta_version: int | None = None  # Resolve once, then pin both runs.

    def __post_init__(self):
        parts=self.registered_model_name.split('.')
        if len(parts)!=3 or any(not p or not p.replace('_','a').isalnum() for p in parts):
            raise ValueError('Use an explicit three-part registered model name')
        if not re.fullmatch(r'[1-9][0-9]*',self.model_version):
            raise ValueError('Use an exact numeric model version, never latest, stage or alias')
        if not re.fullmatch(r'[0-9a-f]{32}',self.training_run_id):
            raise ValueError('Require the exact training run ID')
        if self.input_delta_version is not None and (type(self.input_delta_version) is not int or self.input_delta_version<0):
            raise ValueError('Delta version must be a nonnegative integer')

    @property
    def model_uri(self):return f'models:/{self.registered_model_name}/{self.model_version}'

    @property
    def input_table(self):return f'{self.gold.gold_namespace}.late_delivery_scoring'

    @property
    def output_table(self):return f'{self.gold.gold_namespace}.late_delivery_predictions'


@dataclass(frozen=True)
class VerifiedPipeline:
    pipeline: object
    threshold: float
    cutoff: str
    feature_contract: dict
    training_snapshot: dict
    verification: dict


def verify_pipeline(pipeline, artifacts, params, config):
    """Cross-check saved pipeline and run artifacts; never supply a fallback rule."""
    import numpy as np
    from sklearn.pipeline import Pipeline
    from sklearn.compose import ColumnTransformer
    from sklearn.model_selection import FixedThresholdClassifier
    from .training import derive_acceptance_features
    contract=artifacts['feature_contract.json']
    report=artifacts['evaluation_report.json']
    registry=artifacts['registry_verification.json']
    snapshots=artifacts['input_delta_versions.json']
    if contract!=feature_contract() or report['feature_contract']!=contract:
        raise ValueError('Saved feature contract is incompatible with explicit Gold acceptance predictors')
    if not isinstance(pipeline,Pipeline) or list(pipeline.named_steps)!=['acceptance_features','preprocessing','classifier']:
        raise ValueError('Require the complete saved acceptance/preprocessing/classifier pipeline')
    if pipeline.named_steps['acceptance_features'].func is not derive_acceptance_features:
        raise ValueError('Saved acceptance feature derivation differs from the supported contract')
    if list(pipeline.feature_names_in_)!=contract['raw_predictors']:
        raise ValueError('Pipeline input columns differ from the saved feature contract')
    preprocessing=pipeline.named_steps['preprocessing']
    if (not isinstance(preprocessing,ColumnTransformer) or preprocessing.remainder!='drop'
        or [(name,list(columns)) for name,_,columns in preprocessing.transformers]!=
           [('numeric',contract['numeric']),('categorical',contract['categorical'])]):
        raise ValueError('Saved preprocessing columns differ from the feature contract')
    classifier=pipeline.named_steps['classifier']
    if (not isinstance(classifier,FixedThresholdClassifier) or classifier.response_method!='predict_proba'
        or classifier.pos_label not in (None,1)):
        raise ValueError('Require the saved probability threshold classifier')
    threshold=float(classifier.threshold)
    if not math.isfinite(threshold) or not 0<=threshold<=1:
        raise ValueError('Invalid saved probability threshold')
    if any(float(value)!=threshold for value in
           (report['selected_threshold'],registry['threshold'],params['selected_threshold'])):
        raise ValueError('Pipeline, evaluation, registry and run thresholds conflict')
    cutoff=report['final_training_cutoff']
    boundary=report['partitions']['boundaries']['final_training_cutoff']
    if any(instant(value)!=instant(cutoff) for value in
           (registry['training_cutoff'],params['training_cutoff'],boundary)):
        raise ValueError('Training cutoff artifacts conflict')
    final=report['partitions']['final_train']
    if instant(final['last_outcome_available_at'])>=instant(cutoff) or instant(final['last_acceptance'])>=instant(cutoff):
        raise ValueError('Saved final training used outcomes/acceptances at or after cutoff')
    if report['test_used_for_training'] is not False or params['test_used_for_training']!='False':
        raise ValueError('Saved model does not establish held-out test isolation')
    if (registry['run_id']!=config.training_run_id or registry['registered_model']!=config.registered_model_name
        or str(registry['version'])!=config.model_version or registry['model_uri']!=config.model_uri
        or registry['registry_load_verified'] is not True):
        raise ValueError('Registry verification does not match the exact requested model/run/version')
    source=snapshots['source_contract']
    if (source['reference_date']!=config.gold.bronze.reference_date or source['prefix']!=config.gold.bronze.prefix
        or source['predictors']!=list(PREDICTORS) or source['open_rows']!=600):
        raise ValueError('Training source contract does not match initial LF v2 Gold')
    for entity in ('late_delivery_training','late_delivery_scoring'):
        snapshot=snapshots[entity]
        if (snapshot['table']!=f'{config.gold.gold_namespace}.{entity}'
            or [list(item) for item in snapshot['schema']]!=[list(item) for item in CONTRACTS_GOLD[entity]]
            or type(snapshot['delta_version']) is not int or snapshot['delta_version']<0):
            raise ValueError('Training input Delta provenance is incompatible')
    classes=np.asarray(pipeline.classes_)
    if classes.shape!=(2,) or classes.dtype.kind not in 'biu' or set(classes.tolist())!={0,1}:
        raise ValueError('Saved classes must explicitly represent 0=on-time and 1=late')
    return VerifiedPipeline(pipeline,threshold,cutoff,contract,snapshots,registry)


def score_orders(data, bundle, config, snapshot):
    """Exactly 600 initial opens; bounded driver inference, never fit/fit_transform."""
    import numpy as np
    import pandas as pd
    from .training import model_input
    names=[n for n,_ in CONTRACTS_GOLD['late_delivery_scoring']]
    if list(data.columns)!=names or len(data)!=600 or data.isna().any().any():
        raise ValueError('Require exactly 600 non-null records matching the ordered Gold scoring contract')
    if data['order_id'].duplicated().any() or (data['order_id'].str.strip()=='').any() or (data['customer_id'].str.strip()=='').any():
        raise ValueError('Open order keys must be nonempty and unique')
    if (snapshot['table']!=config.input_table or not snapshot['table_id']
        or type(snapshot['delta_version']) is not int or snapshot['delta_version']<0):
        raise ValueError('Missing or conflicting fixed Delta snapshot identity')
    if config.input_delta_version is not None and snapshot['delta_version']!=config.input_delta_version:
        raise ValueError('Input Delta snapshot differs from configuration')
    ordered=data.sort_values('order_id',kind='stable').reset_index(drop=True)
    accepted=pd.to_datetime(ordered['accepted_at'],utc=True,errors='raise')
    promised=pd.to_datetime(ordered['promised_delivery_at_acceptance'],utc=True,errors='raise')
    reference=pd.Timestamp(config.gold.bronze.reference_date,tz='UTC')
    if (accepted.isna().any() or promised.isna().any() or (promised<=accepted).any()
        or (accepted<pd.Timestamp(bundle.cutoff)).any() or (accepted>reference).any()):
        raise ValueError('Open acceptance/promise violates saved training cutoff or fixed observation time')
    if (ordered['_batch_id']!='initial').any() or (pd.to_datetime(ordered['_reference_date'],utc=True)!=reference).any():
        raise ValueError('Open input must remain the fixed initial reference snapshot')
    x=model_input(ordered)  # Exact ordered predictors only; no metadata/identifiers.
    from .silver import RANGES, DOMAINS
    from .training_contract import ORIGINAL_NUMERIC, CATEGORICAL
    for name in ORIGINAL_NUMERIC:
        low,high=RANGES[name]
        if ((x[name]<float(low))|(x[name]>float(high))).any():
            raise ValueError(f'Predictor outside the original source contract: {name}')
    for name in CATEGORICAL:
        if not x[name].isin(DOMAINS[name]).all():
            raise ValueError(f'Predictor outside the original source domain: {name}')
    if list(x.columns)!=bundle.feature_contract['raw_predictors']:
        raise ValueError('Inference predictors differ from saved feature contract')
    classes=np.asarray(bundle.pipeline.classes_)
    if classes.shape!=(2,) or classes.dtype.kind not in 'biu' or set(classes.tolist())!={0,1}:
        raise ValueError('Unexpected probability class mapping')
    positive=int(np.flatnonzero(classes==1)[0])
    probabilities=np.asarray(bundle.pipeline.predict_proba(x),dtype='float64')
    if (probabilities.shape!=(600,2) or not np.isfinite(probabilities).all()
        or ((probabilities<0)|(probabilities>1)).any()
        or not np.allclose(probabilities.sum(axis=1),1,rtol=0,atol=1e-12)):
        raise ValueError('Invalid prediction probabilities')
    p=probabilities[:,positive]
    prediction=np.asarray(bundle.pipeline.predict(x))
    if prediction.shape!=(600,) or not np.isin(prediction,[0,1]).all() or not np.array_equal(prediction.astype(bool),p>=bundle.threshold):
        raise ValueError('Pipeline prediction conflicts with its registered probability threshold')
    # Stable canonical input digest includes ALL source columns, not just predictors.
    canonical=ordered.copy()
    for name in ('accepted_at','promised_delivery_at_acceptance'):
        canonical[name]=pd.to_datetime(canonical[name],utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    canonical['_reference_date']=pd.to_datetime(canonical['_reference_date'],utc=True).dt.strftime('%Y-%m-%d')
    for name in ('inventory_fill_rate_at_acceptance','warehouse_load_at_acceptance'):
        canonical[name]=canonical[name].map(str)
    digest=hashlib.sha256(canonical.to_json(orient='records',double_precision=15).encode()).hexdigest()
    result=pd.DataFrame({'order_id':ordered['order_id'],'customer_id':ordered['customer_id'],
        'accepted_at':accepted,'late_probability':p,'predicted_late':prediction.astype(bool),
        'decision_threshold':bundle.threshold,'model_name':config.registered_model_name,
        'model_version':config.model_version,'training_run_id':config.training_run_id,
        'training_cutoff':pd.Timestamp(bundle.cutoff),'input_table':snapshot['table'],
        'input_table_id':snapshot['table_id'],'input_delta_version':snapshot['delta_version'],
        'input_fingerprint':digest,'source_prefix':config.gold.bronze.prefix,
        '_batch_id':ordered['_batch_id'],'_reference_date':reference})
    if set(result['order_id'])!=set(data['order_id']):raise ValueError('Scoring lost open order coverage')
    return result.loc[:,[n for n,_ in PREDICTION_CONTRACT]],{
        'rows':600,'alerts':int(prediction.sum()),'classes':classes.tolist(),
        'late_probability_column':positive,'threshold':bundle.threshold,'training_cutoff':bundle.cutoff,
        'input_fingerprint':digest,'input_snapshot':snapshot,
        'score_quantiles':{str(q):float(np.quantile(p,q)) for q in (0,.1,.25,.5,.75,.9,1)},
        'model_uri':config.model_uri,'training_run_id':config.training_run_id}
