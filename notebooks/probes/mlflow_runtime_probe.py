# Standalone read-only Python cell. No project wheel, training, installation or MLflow writes.
# Run in the intended existing AIDP Spark compute; never print environment values/credentials.
import base64
import importlib
import os
import importlib.metadata
import inspect
import json
import platform
import sys
from urllib.parse import urlsplit

report = {'python':sys.version.split()[0], 'architecture':platform.machine(),
          'spark':spark.version, 'packages':{}, 'api_signatures':{}, 'gold':{},
          'training_performed':False, 'open_orders_scored':0, 'mlflow_writes':False}
for name in ('mlflow','scikit-learn','pandas','numpy','pyspark','delta-spark','aidp-lakehouse-to-ai','mlflow-skinny','skops'):
    try:report['packages'][name]=importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:report['packages'][name]=None
for module,attribute in (
    ('mlflow','set_experiment'),('mlflow','start_run'),('mlflow','log_dict'),
    ('mlflow','register_model'),('mlflow.sklearn','log_model'),('mlflow.sklearn','load_model'),
    ('mlflow.sklearn','save_model'),
    ('sklearn.preprocessing','OneHotEncoder'),('sklearn.model_selection','FixedThresholdClassifier'),('sklearn.linear_model','LogisticRegression'),
    ('sklearn.ensemble','RandomForestClassifier')):
    try:report['api_signatures'][module+'.'+attribute]=str(inspect.signature(getattr(importlib.import_module(module),attribute)))
    except Exception as error:report['api_signatures'][module+'.'+attribute]={'error_class':type(error).__name__}
try:
    import mlflow
    report['mlflow_module_version']=getattr(mlflow,'__version__',None)
    report['mlflow_distributions']=importlib.metadata.packages_distributions().get('mlflow',[])
    report['native_auth_marker_present']='MLFLOW_TRACKING_AUTH' in os.environ
    report['tracking_uri_is_none']=mlflow.get_tracking_uri() is None
    report['registry_uri_is_none']=mlflow.get_registry_uri() is None
    report['tracking_scheme']=urlsplit(mlflow.get_tracking_uri() or '').scheme
    report['registry_scheme']=urlsplit(mlflow.get_registry_uri() or '').scheme
    report['uri_classification']={}
    for label,value in [('tracking',mlflow.get_tracking_uri()),('registry',mlflow.get_registry_uri())]:
        known=('aidp','databricks','./mlruns','mlruns','None','')
        report['uri_classification'][label]={'known_alias':value if value in known else None,
            'is_local_path':isinstance(value,str) and value.startswith(('/', '.')),
            'has_whitespace':isinstance(value,str) and any(c.isspace() for c in value)}
    report['mlflow_auth_provider_entrypoints']=[e.name for e in importlib.metadata.entry_points(group='mlflow.request_auth_provider')]
    report['active_run_present']=mlflow.active_run() is not None
    report['serialization_constants']={n:getattr(importlib.import_module('mlflow.sklearn'),n,None)
        for n in ('SERIALIZATION_FORMAT_CLOUDPICKLE','SERIALIZATION_FORMAT_PICKLE','SERIALIZATION_FORMAT_SKOPS')}
    from mlflow.tracking import MlflowClient
    client=MlflowClient()
    report['client_module']=type(client).__module__
    for name in ('search_experiments','search_registered_models'):
        try:
            method=getattr(client,name)
            report['api_signatures']['MlflowClient.'+name]=str(inspect.signature(method))
            method(max_results=1)
            report[name+'_read']='PASS'
        except Exception as error:
            report[name+'_read']={'error_class':type(error).__name__, 'error_code':getattr(error,'error_code',None)}

except Exception as error:
    report['mlflow_read_error_class']=type(error).__name__
    report['mlflow_read_error_code']=getattr(error,'error_code',None)
    # Only known diagnostic phrases; never emit an exception string that might contain a URI/token.
    message=str(error).casefold()
    report['mlflow_error_flags']={key:key in message for key in (
        'local filesystem', 'maintenance mode', 'file store', 'disabled', 'tracking uri',
        'unsupported', 'auth provider', 'databricks', 'credentials', 'not found', 'sqlalchemy',
        'mlflow_allow_file_store', 'cannot', 'authentication')}

for entity in ('order_operations','delivery_kpis','late_delivery_training','late_delivery_scoring'):
    table='lakehouse_demo.gold.'+entity
    # Read-only SQL/DataFrame operations. No changes to tables, files, config, runs or models.
    frame=spark.table(table)
    report['gold'][entity]={'rows':frame.count(),'columns':frame.columns}
print('AIDP_RUNTIME_PROBE_BASE64='+base64.b64encode(json.dumps(report,sort_keys=True).encode()).decode())
