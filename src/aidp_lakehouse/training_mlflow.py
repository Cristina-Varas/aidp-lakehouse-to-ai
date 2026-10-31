"""Native MLflow adapter. No tracking/registry URI changes or silent local fallback."""
from contextlib import nullcontext
from importlib import metadata, resources
import inspect
import platform
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit


def _distribution_version(name):
    try:return metadata.version(name)
    except metadata.PackageNotFoundError:return None


def preflight(config,*,require_aidp=True):
    import mlflow
    import mlflow.sklearn
    from mlflow.tracking import MlflowClient
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.model_selection import FixedThresholdClassifier
    if require_aidp and platform.machine().lower() in ('arm64','aarch64'):
        raise RuntimeError('Oracle currently requires Intel/AMD compute for native Experiments; attach supported compute')
    functions=[mlflow.set_experiment,mlflow.start_run,mlflow.log_params,mlflow.log_metrics,
               mlflow.log_dict,mlflow.register_model,mlflow.sklearn.log_model,mlflow.sklearn.load_model]
    if not all(callable(f) for f in functions):raise RuntimeError('Required MLflow API missing')
    parameters=inspect.signature(mlflow.sklearn.log_model).parameters
    key='name' if 'name' in parameters else 'artifact_path' if 'artifact_path' in parameters else None
    if key is None or not {'signature','code_paths','pip_requirements'}<=set(parameters):
        raise RuntimeError('Runtime lacks complete sklearn pipeline logging APIs; inspect AIDP sample code before continuing')
    if 'sparse_output' not in inspect.signature(OneHotEncoder).parameters:
        raise RuntimeError('Runtime requires a supported sklearn OneHotEncoder(sparse_output=...) API')
    native_marker='MLFLOW_TRACKING_AUTH' in os.environ
    distribution_version=_distribution_version('mlflow')
    if require_aidp and not native_marker:
        raise RuntimeError('Native AIDP MLflow auth integration is not present; do not substitute another backend')
    for uri in (mlflow.get_tracking_uri(),mlflow.get_registry_uri()):
        scheme=urlsplit(uri or '').scheme.lower()
        # The observed scheme-less values resolved to FileStore despite the
        # native auth marker. Never treat auth presence as proof of remote routing.
        if require_aidp and scheme in ('','file','sqlite'):
            raise RuntimeError('Native MLflow tracking/registry does not identify a remote backend. '
                'Review the AIDP Experiments binding; no local fallback is allowed. No training started.')
    serialization=parameters['serialization_format'].default
    if serialization=='skops':
        import skops.io
        if 'skops_trusted_types' not in parameters:
            raise RuntimeError('Native skops logging lacks explicit project-function trust support')
    elif serialization not in ('cloudpickle','pickle'):
        raise RuntimeError('Unsupported model serialization default; preserve native configuration')
    if mlflow.active_run() is not None:raise RuntimeError('End the existing MLflow run before this independent notebook run')
    # Read-only authenticated capability checks, never print credentials or URIs.
    try:
        client=MlflowClient()
        client.search_experiments(max_results=1)
        client.search_registered_models(max_results=1)
    except Exception as error:
        if 'mlflow_allow_file_store' in str(error).casefold():
            raise RuntimeError('Native MLflow resolves a filesystem backend in maintenance mode. '
                'Review the AIDP Experiments tracking/registry binding; do not enable a local file-store '
                'fallback or overwrite native auth/URIs. No training started.') from None
        raise RuntimeError('Native MLflow client is not ready ('+type(error).__name__+
            '). Use the AIDP Experiments integration; preserve native auth/URIs. No training started.') from None
    return dict(versions={name:_distribution_version(name) for name in ('mlflow','mlflow-skinny','scikit-learn','pandas','numpy','skops')},
                mlflow_module_version=getattr(mlflow,'__version__',None),native_auth_marker_present=native_marker,
                native_serialization_default=serialization,mlflow_distribution_version=distribution_version,
                model_log_path_argument=key,tracking_checked=True,registry_read_checked=True,
                environment='AIDP runtime preflight' if require_aidp else 'explicit local test backend',
                registry_write_and_load='verified only after registration; not established by read-only preflight')


def _metrics(prefix,values):
    return {prefix+'_'+k:float(v) for k,v in values.items() if isinstance(v,(int,float)) and not isinstance(v,bool)}


def log_register_verify(result,config,snapshots,capabilities,*,_owned_run_id=None):
    import mlflow
    import mlflow.sklearn
    import numpy as np
    from mlflow.models import infer_signature
    from mlflow.tracking import MlflowClient
    from . import __version__
    from .training_contract import feature_contract
    active=mlflow.active_run()
    if active is not None and active.info.run_id!=_owned_run_id:
        raise RuntimeError('Refuse to nest inside or finish an unrelated run')
    if _owned_run_id is not None and active is None:raise RuntimeError('Owned tracking run is no longer active')
    if active is None:mlflow.set_experiment(config.experiment_name)
    context=nullcontext(active) if active is not None else mlflow.start_run(run_name='initial-late-delivery-temporal')
    requirements=[f'{name}=={_distribution_version(name)}' for name in
                  ('scikit-learn','pandas','numpy','cloudpickle') if _distribution_version(name)]
    # Record the installed distribution: AIDP supplies mlflow-skinny without
    # the full mlflow distribution. Never replace its native client integration.
    if capabilities.get('mlflow_distribution_version'):
        requirements.append('mlflow=='+capabilities['mlflow_distribution_version'])
    elif capabilities.get('versions',{}).get('mlflow-skinny'):
        requirements.append('mlflow-skinny=='+capabilities['versions']['mlflow-skinny'])
    if capabilities.get('native_serialization_default')=='skops' and _distribution_version('skops'):
        requirements.append('skops=='+_distribution_version('skops'))
    mlflow.sklearn.autolog(disable=True)  # Explicit logging; avoid accidental evaluation/autolog payloads.
    with tempfile.TemporaryDirectory(prefix='aidp-model-code-') as temp:
        package=Path(temp)/'aidp_lakehouse';package.mkdir()
        # Embed only package Python code; no checkout credentials, datasets or local configuration.
        for child in resources.files('aidp_lakehouse').iterdir():
            if child.name.endswith('.py'):(package/child.name).write_bytes(child.read_bytes())
        with context as run:
            mlflow.log_params({'seed':config.seed,'validation_start':config.validation_start,
                'test_start':config.test_start,'training_cutoff':result.report['final_training_cutoff'],
                'selected_candidate':result.report['selected_candidate']['name'],
                'selected_threshold':result.report['selected_threshold'],'package_version':__version__,
                'selection_metric':'validation_average_precision','threshold_metric':'validation_f1',
                'candidate_count':len(result.report['validation_candidates']),'test_used_for_training':False})
            mlflow.log_dict(snapshots,'input_delta_versions.json')
            mlflow.log_dict(feature_contract(),'feature_contract.json')
            mlflow.log_dict(result.report,'evaluation_report.json')
            mlflow.log_dict(capabilities,'runtime_capabilities.json')
            mlflow.log_dict({'pip_requirements':requirements},'environment.json')
            for name,part in result.report['partitions'].items():
                if isinstance(part,dict) and 'rows' in part:
                    mlflow.log_metrics({f'{name}_rows':part['rows'],f'{name}_prevalence':part['prevalence']})
            for row in result.report['validation_candidates']:
                name=row['candidate']['name']
                mlflow.log_params({f'{name}_{k}':v for k,v in row['candidate'].items()})
                mlflow.log_metrics(_metrics('validation_'+name,row['metrics']))
            for name,values in result.report['test'].items():
                mlflow.log_metrics(_metrics('test_'+name,values))
            sample=result.verification_input
            signature=infer_signature(sample,result.model.predict(sample))
            kwargs={capabilities['model_log_path_argument']:'selected_pipeline'}
            if capabilities.get('native_serialization_default')=='skops':
                # Exact known project function only; never trust arbitrary types
                # reported by an unknown artifact or downgrade native serialization.
                kwargs['skops_trusted_types']=['aidp_lakehouse.training.derive_acceptance_features']
            info=mlflow.sklearn.log_model(result.model,signature=signature,code_paths=[str(package)],
                                         pip_requirements=requirements,**kwargs)
            model_uri=info.model_uri
            registered=mlflow.register_model(model_uri,config.registered_model_name,await_registration_for=60)
            version=str(registered.version)
            exact_uri=f'models:/{config.registered_model_name}/{version}'
            record=MlflowClient().get_model_version(config.registered_model_name,version)
            if record.run_id!=run.info.run_id:raise RuntimeError('Registry version belongs to a different run')
            loaded=mlflow.sklearn.load_model(exact_uri)
            if not np.allclose(loaded.predict_proba(sample),result.verification_probabilities,rtol=0,atol=1e-12):
                raise RuntimeError('Registered-version probabilities differ from the evaluated pipeline')
            if not np.array_equal(loaded.predict(sample),result.model.predict(sample)):
                raise RuntimeError('Registered pipeline did not preserve its selected threshold')
            verification=dict(run_id=run.info.run_id,experiment_id=run.info.experiment_id,
                backend_environment=capabilities['environment'],
                registered_model=config.registered_model_name,version=version,model_uri=exact_uri,
                registry_load_verified=True,threshold=result.report['selected_threshold'],
                training_cutoff=result.report['final_training_cutoff'],verification_rows='5 held-out historical rows; zero open orders')
            mlflow.log_dict(verification,'registry_verification.json')
            return verification


def run_experiment(data,config,first_open_acceptance,snapshots,capabilities):
    """Track the actual fit/evaluation interval, then register within the same owned run."""
    import mlflow
    import mlflow.sklearn
    from .training import train_and_evaluate
    if mlflow.active_run() is not None:raise RuntimeError('Do not adopt an unrelated active MLflow run')
    mlflow.set_experiment(config.experiment_name)
    mlflow.sklearn.autolog(disable=True)  # Explicit experiment logging, not a native URI/auth change.
    with mlflow.start_run(run_name='initial-late-delivery-temporal') as run:
        mlflow.log_dict(snapshots,'input_delta_versions.json')
        result=train_and_evaluate(data,config,first_open_acceptance)
        registry=log_register_verify(result,config,snapshots,capabilities,_owned_run_id=run.info.run_id)
        return result,registry
