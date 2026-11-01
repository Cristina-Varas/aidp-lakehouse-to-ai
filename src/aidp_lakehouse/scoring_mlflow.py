"""Read-only native registry loading of one exact, user-verified model version."""
from .scoring import verify_pipeline
from .training_mlflow import preflight

ARTIFACTS=('feature_contract.json','evaluation_report.json','registry_verification.json','input_delta_versions.json')


def load_verified_pipeline(config,*,require_aidp=True):
    import json
    from pathlib import Path
    import tempfile
    import mlflow.sklearn
    from mlflow.tracking import MlflowClient
    capabilities=preflight(config,require_aidp=require_aidp)
    client=MlflowClient()
    record=client.get_model_version(config.registered_model_name,config.model_version)
    if (record.name!=config.registered_model_name or str(record.version)!=config.model_version
        or record.run_id!=config.training_run_id):
        raise ValueError('Requested registry version belongs to a different model or training run')
    run=client.get_run(config.training_run_id)
    if run.info.status!='FINISHED':raise ValueError('Training run must be successfully finished')
    artifacts={}
    with tempfile.TemporaryDirectory(prefix='aidp-scoring-contract-') as directory:
        for name in ARTIFACTS:
            local=client.download_artifacts(config.training_run_id,name,directory)
            artifacts[name]=json.loads(Path(local).read_text())
    # Native client retains its own artifact authentication and serialization policy.
    pipeline=mlflow.sklearn.load_model(config.model_uri)
    bundle=verify_pipeline(pipeline,artifacts,run.data.params,config)
    return bundle,capabilities
