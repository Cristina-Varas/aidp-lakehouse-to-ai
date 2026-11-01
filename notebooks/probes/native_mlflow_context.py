# Standalone READ-ONLY cell for the existing successful 04 kernel or failing 05 kernel.
# Do not run training/scoring cells to compare their native context.
import importlib.metadata
import json
import os
from urllib.parse import urlsplit
import mlflow

report={'mlflow_version':mlflow.__version__,'native_auth_selected':
        os.environ.get('MLFLOW_TRACKING_AUTH')=='mlops_auth_provider',
        'model_loaded':False,'scoring_performed':False,'mlflow_writes':False}
for label,value in [('tracking',mlflow.get_tracking_uri()),('registry',mlflow.get_registry_uri())]:
    parsed=urlsplit(value or '')
    # No path, username, password, query, fragment or full environment values.
    report[label]={'configured':bool(value),'scheme':parsed.scheme,'hostname':parsed.hostname}
report['native_provider_installed']=any(
    ep.name=='mlops_auth_provider' and ep.value=='mlops_mlflow_plugin.plugins.mlops_auth_provider:MLOpsAuthProvider'
    for ep in importlib.metadata.entry_points(group='mlflow.request_auth_provider'))
try:report['spark_principal_present']=bool(spark.sparkContext.getLocalProperty('dh-user-principal'))
except NameError:report['spark_principal_present']=False
print(json.dumps(report,indent=2))
