"""Verified AIDP REST binding with native request authentication; metadata reads only.

Route bases must come from successful documented API calls, never a Workbench
UI URL. No environment/auth/provider/dependency changes or local fallback.
"""
from dataclasses import dataclass
from importlib import metadata
import os
import re
from urllib.parse import urlsplit

NATIVE_PROVIDER = 'mlops_auth_provider'
NATIVE_ENTRY_POINT = 'mlops_mlflow_plugin.plugins.mlops_auth_provider:MLOpsAuthProvider'
PROJECT_HOST = 'datalake.eu-frankfurt-1.oci.oraclecloud.com'


@dataclass(frozen=True)
class NativeBackendConfig:
    tracking_uri: str
    registry_uri: str
    experiment_id: str = '1'
    experiment_name: str = 'lakehouse_to_ai_late_delivery'

    def __post_init__(self):
        paths=[]
        for uri in (self.tracking_uri,self.registry_uri):
            parsed=urlsplit(uri)
            if (parsed.scheme!='https' or parsed.netloc!=PROJECT_HOST or parsed.username or parsed.password
                or parsed.query or parsed.fragment or '%' in parsed.path):
                raise ValueError('Require the verified Frankfurt AIDP service bases without credentials/query/fragment')
            paths.append(parsed.path)
        registry=re.fullmatch(r'/20260430/aiDataPlatforms/(ocid1\.[A-Za-z0-9._-]+)/mlops',paths[1])
        if not registry:raise ValueError('Registry must use the documented instance-scoped MLOps base')
        expected='/20260430/aiDataPlatforms/'+registry[1]+'/workspaces/'
        if not (paths[0].startswith(expected) and re.fullmatch(re.escape(expected)+r'[A-Za-z0-9_-]+/mlops',paths[0])):
            raise ValueError('Tracking must use the verified workspace-scoped base in the same instance')
        if self.experiment_id!='1' or self.experiment_name!='lakehouse_to_ai_late_delivery':
            raise ValueError('Require the verified project experiment identity')


def uri_shape(value):
    """Safe diagnostics; do not return a URI, credential, query or environment value."""
    parsed=urlsplit(value or '')
    return {'configured':bool(value),'scheme':parsed.scheme,'has_host':bool(parsed.hostname)}


def binding_diagnostics():
    import mlflow
    return {'mlflow_version':getattr(mlflow,'__version__',None),
        'native_auth_selected':os.environ.get('MLFLOW_TRACKING_AUTH')==NATIVE_PROVIDER,
        'tracking':uri_shape(mlflow.get_tracking_uri()),'registry':uri_shape(mlflow.get_registry_uri()),
        'tracking_environment':uri_shape(os.environ.get('MLFLOW_TRACKING_URI')),
        'registry_environment':uri_shape(os.environ.get('MLFLOW_REGISTRY_URI'))}


def _read_identity(client,binding,config):
    experiment=client.get_experiment(binding.experiment_id)
    run=client.get_run(config.training_run_id)
    model=client.get_model_version(config.registered_model_name,config.model_version)
    if (str(experiment.experiment_id)!=binding.experiment_id or experiment.name!=binding.experiment_name
        or str(run.info.experiment_id)!=binding.experiment_id or run.info.run_id!=config.training_run_id
        or run.info.status!='FINISHED' or model.name!=config.registered_model_name
        or str(model.version)!=config.model_version or model.run_id!=config.training_run_id or model.status!='READY'):
        raise ValueError('Native backend returned a different experiment/run/model identity or unfinished state')
    return {'experiment_id':binding.experiment_id,'experiment_name':experiment.name,
        'training_run_id':run.info.run_id,'run_status':run.info.status,
        'model_name':model.name,'model_version':str(model.version),'model_status':model.status}


def initialize_native_backend(binding,config):
    """Read-verify native routing first, then bind and verify the default client.

    Authentication remains the platform's Spark/Java resource-principal provider.
    Does not load a pipeline, fit, predict, write an experiment or change env vars.
    """
    import mlflow
    from mlflow.tracking import MlflowClient
    if os.environ.get('MLFLOW_TRACKING_AUTH')!=NATIVE_PROVIDER:
        raise RuntimeError('Native mlops_auth_provider is not selected. Restore the AIDP session binding; do not replace authentication.')
    providers=metadata.entry_points(group='mlflow.request_auth_provider')
    if not any(p.name==NATIVE_PROVIDER and p.value==NATIVE_ENTRY_POINT for p in providers):
        raise RuntimeError('The inspected native AIDP auth provider is unavailable; restore native compute libraries')
    if mlflow.active_run() is not None:
        raise RuntimeError('Do not change native routing while a tracking run is active')
    before=binding_diagnostics()
    # Preserve a working platform-injected HTTPS binding (including internal
    # native service hosts). Never replace it merely because an API base differs.
    current=(mlflow.get_tracking_uri(),mlflow.get_registry_uri())
    remote=lambda uri: urlsplit(uri or '').scheme in ('https','http') and bool(urlsplit(uri or '').hostname)
    existing_error=None
    if all(remote(uri) for uri in current):
        try:
            identity=_read_identity(MlflowClient(),binding,config)
        except Exception as error:
            existing_error=type(error).__name__
        else:
            return {'status':'NATIVE CONNECTION PASS','identity':identity,'before':before,
                'after':binding_diagnostics(),'route_source':'existing_verified_native_binding',
                'authentication_changed':False,'model_loaded':False,'scoring_performed':False,'mlflow_writes':False}
    try:
        client=MlflowClient(tracking_uri=binding.tracking_uri,registry_uri=binding.registry_uri)
        identity=_read_identity(client,binding,config)
    except Exception as error:
        body=getattr(error,'json',{})
        platform_code=body.get('code') if isinstance(body,dict) else None
        known_code=platform_code if platform_code in ('NotAuthorizedOrNotFound','NotAuthenticated','Forbidden') else type(error).__name__
        raise RuntimeError('AIDP native metadata verification failed ('+known_code+'). '
            'No global URI or auth setting was changed. Restore this notebook session\'s native '
            'instance/workspace routing and authorized Spark principal for lakehouse_to_ai; '
            'see docs/aidp-native-mlflow-binding.md. No model loaded or scoring performed.') from None
    original=(mlflow.get_tracking_uri(),mlflow.get_registry_uri())
    try:
        mlflow.set_tracking_uri(binding.tracking_uri)
        mlflow.set_registry_uri(binding.registry_uri)
        effective=_read_identity(MlflowClient(),binding,config)
        if effective!=identity or os.environ.get('MLFLOW_TRACKING_AUTH')!=NATIVE_PROVIDER:
            raise RuntimeError('Native binding changed during initialization')
    except BaseException:
        # URI rollback does not construct or enable FileStore; auth/env stay untouched.
        mlflow.set_tracking_uri(original[0]);mlflow.set_registry_uri(original[1])
        raise RuntimeError('Default native client verification failed; previous URI settings restored. No scoring performed.') from None
    return {'status':'NATIVE CONNECTION PASS','identity':identity,'before':before,
        'after':binding_diagnostics(),'route_source':'verified_documented_api_bases',
        'existing_binding_read_error_class':existing_error,'authentication_changed':False,'model_loaded':False,
        'scoring_performed':False,'mlflow_writes':False}
