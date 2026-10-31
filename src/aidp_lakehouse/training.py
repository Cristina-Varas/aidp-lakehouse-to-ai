"""Bounded, deterministic sklearn training. No cloud I/O or scoring of open orders."""
from dataclasses import asdict, dataclass
import hashlib
import json
import warnings
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score,roc_auc_score,precision_score,recall_score,
                             f1_score,confusion_matrix)
from sklearn.model_selection import FixedThresholdClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler
from .gold import PREDICTORS, CONTRACTS_GOLD
from .training_contract import (TrainingConfig,CATEGORICAL,ORIGINAL_NUMERIC,NUMERIC,
                                CANDIDATES,THRESHOLDS,feature_contract)


def model_input(frame):
    """Explicit model boundary: no IDs, labels, outcome metadata or current fields."""
    result=frame.loc[:,list(PREDICTORS)].copy()
    for name in ORIGINAL_NUMERIC:
        result[name]=pd.to_numeric(result[name],errors='raise').astype('float64')
        if not np.isfinite(result[name]).all():raise ValueError(f'Non-finite predictor: {name}')
    for name in ('accepted_at','promised_delivery_at_acceptance'):
        times=pd.to_datetime(result[name],utc=True,errors='raise')
        if times.isna().any():raise ValueError('Null predictor timestamp')
        result[name]=times.dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    for name in CATEGORICAL:
        if result[name].isna().any() or not result[name].map(lambda v:isinstance(v,str) and bool(v.strip())).all():
            raise ValueError(f'Missing/invalid categorical predictor: {name}')
    return result


def derive_acceptance_features(raw):
    if list(raw.columns)!=list(PREDICTORS):
        raise ValueError('Pipeline requires exactly the explicit ordered Gold predictors')
    x=model_input(raw)
    derived=x.loc[:,list((*CATEGORICAL,*ORIGINAL_NUMERIC))].copy()
    lead=(pd.to_datetime(x['promised_delivery_at_acceptance'],utc=True)-
          pd.to_datetime(x['accepted_at'],utc=True)).dt.total_seconds()/86400
    if (lead<=0).any() or not np.isfinite(lead).all():raise ValueError('Original promise must follow acceptance')
    derived['promised_lead_days']=lead
    return derived


def make_pipeline(candidate,seed,threshold=0.5):
    kind=candidate['kind']
    if kind=='baseline':estimator=DummyClassifier(strategy='prior')
    elif kind=='logistic':estimator=LogisticRegression(C=candidate['C'],max_iter=500,tol=1e-6,random_state=seed)
    elif kind=='forest':estimator=RandomForestClassifier(n_estimators=candidate['n_estimators'],
        max_depth=candidate['max_depth'],min_samples_leaf=candidate['min_samples_leaf'],random_state=seed,n_jobs=1)
    else:raise ValueError('Unknown fixed candidate')
    preprocessor=ColumnTransformer([
        ('numeric',StandardScaler(),list(NUMERIC)),
        ('categorical',OneHotEncoder(handle_unknown='ignore',sparse_output=True),list(CATEGORICAL)),
    ],remainder='drop',sparse_threshold=1.0)
    return Pipeline([('acceptance_features',FunctionTransformer(derive_acceptance_features,validate=False)),
                     ('preprocessing',preprocessor),('classifier',FixedThresholdClassifier(estimator,threshold=threshold,response_method='predict_proba'))])


def binary_metrics(labels,probabilities,threshold):
    y=np.asarray(labels,dtype='int64');p=np.asarray(probabilities,dtype='float64')
    if not len(y) or len(y)!=len(p) or not np.isfinite(p).all() or ((p<0)|(p>1)).any():
        raise ValueError('Invalid evaluation labels/probabilities')
    if not set(np.unique(y))<={0,1}:raise ValueError('Labels must be binary')
    predicted=(p>=threshold).astype('int64')
    matrix=confusion_matrix(y,predicted,labels=[0,1])
    both=len(np.unique(y))==2
    return dict(roc_auc=float(roc_auc_score(y,p)) if both else None,
                average_precision=float(average_precision_score(y,p)) if y.sum()>0 else None,
                precision=float(precision_score(y,predicted,zero_division=0)),
                recall=float(recall_score(y,predicted,zero_division=0)),
                f1=float(f1_score(y,predicted,zero_division=0)),
                confusion_matrix=matrix.tolist(),tn=int(matrix[0,0]),fp=int(matrix[0,1]),
                fn=int(matrix[1,0]),tp=int(matrix[1,1]),threshold=float(threshold),
                roc_auc_status='defined' if both else 'undefined_single_class',
                average_precision_status='defined' if y.sum()>0 else 'undefined_no_positive_labels')


def choose_threshold(y,p):
    scores=[binary_metrics(y,p,t) for t in THRESHOLDS]
    # F1 first, then higher threshold (fewer alerts); predetermined tie policy.
    best=max(scores,key=lambda r:(r['f1'],r['threshold']))
    return best,scores


def _summary(frame):
    return dict(rows=len(frame),first_acceptance=frame['accepted_at'].min().isoformat(),
                last_acceptance=frame['accepted_at'].max().isoformat(),
                last_outcome_available_at=frame['outcome_available_at'].max().isoformat(),
                positives=int(frame['is_late'].sum()),prevalence=float(frame['is_late'].mean()))


def temporal_partitions(data,config,first_open_acceptance):
    required=[n for n,_ in CONTRACTS_GOLD['late_delivery_training']]
    if list(data.columns)!=required:raise ValueError('Training data must match the exact ordered Gold contract')
    if len(data)>config.max_training_rows:raise ValueError('Training exceeds the explicit driver-memory row guard')
    df=data.copy()
    if df.isna().any().any() or df['order_id'].duplicated().any():raise ValueError('Null or duplicate Gold training records')
    if not df['is_late'].isin([True,False]).all():raise ValueError('Invalid Gold binary label')
    for name in ('accepted_at','promised_delivery_at_acceptance','outcome_available_at'):
        df[name]=pd.to_datetime(df[name],utc=True,errors='raise')
        if df[name].isna().any():raise ValueError('Invalid/null Gold training timestamp')
    observation=pd.Timestamp('2026-10-28T00:00:00Z')
    val=pd.Timestamp(config.validation_start);test=pd.Timestamp(config.test_start)
    first_open=pd.Timestamp(first_open_acceptance)
    if first_open.tzinfo is None:raise ValueError('First open acceptance needs timezone')
    first_open=first_open.tz_convert('UTC')
    if not test<=first_open<=observation:raise ValueError('Final training cutoff must precede first open acceptance')
    if (df['accepted_at']>=first_open).any() or (df['outcome_available_at']>observation).any():
        raise ValueError('Historical cohort crosses first-open acceptance or observation cutoff')
    if (df['outcome_available_at']<df['accepted_at']).any():raise ValueError('Outcome cannot precede acceptance')
    if (df['_batch_id']!='initial').any() or (pd.to_datetime(df['_reference_date'],utc=True).dt.date!=observation.date()).any():
        raise ValueError('Gold initial/reference snapshot differs')
    model_input(df)  # Validate feature types, without fitting any preprocessing.
    derive_acceptance_features(model_input(df))
    df=df.sort_values(['accepted_at','order_id'],kind='stable').reset_index(drop=True)
    train_mask=df['accepted_at']<val
    val_mask=(df['accepted_at']>=val)&(df['accepted_at']<test)
    test_mask=df['accepted_at']>=test
    parts={'train':df[train_mask&(df['outcome_available_at']<val)].copy(),
           'validation':df[val_mask&(df['outcome_available_at']<test)].copy(),
           'test':df[test_mask].copy(),
           'final_train':df[(df['accepted_at']<test)&(df['outcome_available_at']<test)].copy()}
    for name,frame in parts.items():
        if frame.empty:raise ValueError(f'Empty temporal partition: {name}')
        if name!='test' and frame['is_late'].nunique()!=2:raise ValueError(f'{name} needs both classes')
    report={name:_summary(frame) for name,frame in parts.items()}
    report['boundaries']={'validation_start':val.isoformat(),'test_start':test.isoformat(),
        'final_training_cutoff':test.isoformat(),'first_open_acceptance':first_open.isoformat(),
        'test_observation_cutoff':observation.isoformat(),'outcome_rule':'strictly before training cutoff'}
    report['excluded_unavailable_labels']={'train':int((train_mask&(df['outcome_available_at']>=val)).sum()),
        'validation':int((val_mask&(df['outcome_available_at']>=test)).sum())}
    report['cohort_reconciliation']={'total':len(df),'train':len(parts['train']),'validation':len(parts['validation']),
        'test':len(parts['test']),'purged':sum(report['excluded_unavailable_labels'].values())}
    return parts,report


@dataclass
class TrainingResult:
    model: object
    baseline: object
    report: dict
    verification_input: object
    verification_probabilities: object


def train_and_evaluate(data,config:TrainingConfig,first_open_acceptance):
    parts,partition_report=temporal_partitions(data,config,first_open_acceptance)
    x_train=model_input(parts['train']);y_train=parts['train']['is_late'].astype('int64')
    x_val=model_input(parts['validation']);y_val=parts['validation']['is_late'].astype('int64')
    validation=[]
    # Convergence errors are surfaced, not hidden as successful trained candidates.
    for candidate in CANDIDATES:
        pipeline=make_pipeline(candidate,config.seed)
        with warnings.catch_warnings():
            warnings.simplefilter('error',ConvergenceWarning)
            pipeline.fit(x_train,y_train)
        best,curve=choose_threshold(y_val,pipeline.predict_proba(x_val)[:,1])
        validation.append(dict(candidate=dict(candidate),metrics=best,threshold_search=curve))
    winner=max(range(len(validation)),key=lambda i:(validation[i]['metrics']['average_precision'],
                validation[i]['metrics']['roc_auc'],-i))
    selected=validation[winner];chosen=selected['candidate'];threshold=selected['metrics']['threshold']
    # Final refit uses ONLY pre-test accepted orders whose outcomes were already known.
    # The held-out test is never used for fitting, selection, or threshold selection.
    x_final=model_input(parts['final_train']);y_final=parts['final_train']['is_late'].astype('int64')
    final=make_pipeline(chosen,config.seed,threshold)
    baseline_threshold=validation[0]['metrics']['threshold']
    baseline=make_pipeline(CANDIDATES[0],config.seed,baseline_threshold)
    with warnings.catch_warnings():
        warnings.simplefilter('error',ConvergenceWarning)
        final.fit(x_final,y_final);baseline.fit(x_final,y_final)
    x_test=model_input(parts['test']);y_test=parts['test']['is_late'].astype('int64')
    selected_metrics=binary_metrics(y_test,final.predict_proba(x_test)[:,1],threshold)
    baseline_metrics=binary_metrics(y_test,baseline.predict_proba(x_test)[:,1],baseline_threshold)
    delta={k:(selected_metrics[k]-baseline_metrics[k] if selected_metrics[k] is not None and baseline_metrics[k] is not None else None)
           for k in ('roc_auc','average_precision','precision','recall','f1')}
    membership={name:hashlib.sha256('\n'.join(frame['order_id']).encode()).hexdigest() for name,frame in parts.items()}
    report=dict(partitions=partition_report,validation_candidates=validation,selected_candidate=chosen,
                selected_threshold=threshold,selection_rule='validation AP; then ROC-AUC; then fixed candidate order',
                threshold_rule='validation F1 on fixed grid; ties choose higher threshold',
                test={'selected':selected_metrics,'prevalence_baseline':baseline_metrics,'selected_minus_baseline':delta},
                seed=config.seed,feature_contract=feature_contract(),partition_membership_sha256=membership,
                final_training_cutoff=config.test_start,test_used_for_training=False,
                future_open_orders_scored=0)
    sample=x_test.iloc[:5].copy()  # Historical rows only, for registry round-trip checks.
    return TrainingResult(final,baseline,report,sample,final.predict_proba(sample))
