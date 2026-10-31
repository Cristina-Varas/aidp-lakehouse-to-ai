"""Real MLflow local SQLite registry integration; never connects to AIDP."""
from importlib.util import find_spec
from pathlib import Path
import tempfile
import unittest

HAS_ML=all(find_spec(n) is not None for n in ('mlflow','numpy','pandas','sklearn'))


@unittest.skipUnless(HAS_ML,'Requires optional MLflow training environment')
class MlflowTests(unittest.TestCase):
    def test_complete_pipeline_register_and_load_exact_version(self):
        import mlflow
        import numpy as np
        from test_training import fixture
        from aidp_lakehouse.training import train_and_evaluate
        from aidp_lakehouse.training_contract import TrainingConfig
        from aidp_lakehouse.training_mlflow import preflight,log_register_verify,run_experiment
        from mlflow.tracking import MlflowClient
        with tempfile.TemporaryDirectory(prefix='aidp-training-mlflow-') as directory:
            tracking_before,registry_before=mlflow.get_tracking_uri(),mlflow.get_registry_uri()
            uri='sqlite:///'+str(Path(directory)/'tracking.db')
            try:
                mlflow.set_tracking_uri(uri);mlflow.set_registry_uri(uri)
                cfg=TrainingConfig()
                mlflow.create_experiment(cfg.experiment_name,artifact_location=(Path(directory)/'artifacts').as_uri())
                # Explicit local mode only in tests. Production notebook always requires native AIDP.
                capabilities=preflight(cfg,require_aidp=False)
                result,logged=run_experiment(fixture(),cfg,'2026-10-22T00:00:00Z',
                     {'training':{'delta_version':7},'scoring':{'delta_version':2}},capabilities)
                self.assertEqual(logged['version'],'1');self.assertTrue(logged['registry_load_verified'])
                loaded=mlflow.sklearn.load_model(logged['model_uri'])
                np.testing.assert_array_equal(loaded.predict(result.verification_input),result.model.predict(result.verification_input))
                pyfunc=mlflow.pyfunc.load_model(logged['model_uri'])
                np.testing.assert_array_equal(pyfunc.predict(result.verification_input),result.model.predict(result.verification_input))
                client=MlflowClient()
                run=client.get_run(logged['run_id'])
                self.assertEqual(run.info.status,'FINISHED')
                self.assertIn('test_selected_average_precision',run.data.metrics)
                self.assertEqual(run.data.params['test_used_for_training'],'False')
                artifacts={a.path for a in client.list_artifacts(logged['run_id'])}
                self.assertTrue({'input_delta_versions.json','feature_contract.json','evaluation_report.json',
                                 'registry_verification.json'}<=artifacts)
                # Confirm no code/dataset/config from the workspace is packaged with the model.
                downloaded=Path(mlflow.artifacts.download_artifacts(artifact_uri=logged['model_uri'],
                                                                      dst_path=str(Path(directory)/'downloaded')))
                embedded=list((downloaded/'code/aidp_lakehouse').iterdir())
                self.assertTrue(embedded);self.assertTrue(all(p.suffix=='.py' for p in embedded if p.is_file()))
                self.assertFalse(any(p.name in ('data','generated','.env') for p in embedded))
            finally:
                if mlflow.active_run():mlflow.end_run(status='FAILED')
                mlflow.set_tracking_uri(tracking_before);mlflow.set_registry_uri(registry_before)

    def test_production_preflight_rejects_local_backend(self):
        import mlflow
        from unittest.mock import patch
        from aidp_lakehouse.training_contract import TrainingConfig
        from aidp_lakehouse.training_mlflow import preflight
        with patch('platform.machine',return_value='x86_64'), patch.dict(__import__('os').environ,{'MLFLOW_TRACKING_AUTH':'native-test-marker'}), \
             patch.object(mlflow,'get_tracking_uri',return_value='file:///tmp/forbidden-fallback'):
            with self.assertRaisesRegex(RuntimeError,'no local fallback'):preflight(TrainingConfig())

    def test_skops_roundtrip_uses_only_explicit_project_function_trust(self):
        if find_spec('skops') is None:self.skipTest('skops is optional outside native-format tests')
        import skops.io
        import numpy as np
        from test_training import fixture
        from aidp_lakehouse.training import train_and_evaluate
        from aidp_lakehouse.training_contract import TrainingConfig
        result=train_and_evaluate(fixture(),TrainingConfig(),'2026-10-22T00:00:00Z')
        encoded=skops.io.dumps(result.model)
        unknown=skops.io.get_untrusted_types(data=encoded)
        self.assertEqual(set(unknown),{'aidp_lakehouse.training.derive_acceptance_features'})
        loaded=skops.io.loads(encoded,trusted=['aidp_lakehouse.training.derive_acceptance_features'])
        np.testing.assert_array_equal(loaded.predict_proba(result.verification_input),result.verification_probabilities)
        np.testing.assert_array_equal(loaded.predict(result.verification_input),result.model.predict(result.verification_input))

    def test_native_skinny_metadata_with_ready_remote_binding_is_not_replaced(self):
        import inspect,os
        import mlflow,mlflow.sklearn
        from unittest.mock import patch,Mock
        from aidp_lakehouse.training_contract import TrainingConfig
        from aidp_lakehouse.training_mlflow import preflight,_distribution_version
        real_version=_distribution_version
        version=lambda name:None if name=='mlflow' else real_version(name)
        signature=inspect.Signature([
            inspect.Parameter('sk_model',inspect.Parameter.POSITIONAL_OR_KEYWORD),
            *[inspect.Parameter(n,inspect.Parameter.KEYWORD_ONLY,default=None)
              for n in ('name','code_paths','pip_requirements','signature','skops_trusted_types')],
            inspect.Parameter('serialization_format',inspect.Parameter.KEYWORD_ONLY,default='skops')])
        client=Mock()
        with patch('platform.machine',return_value='x86_64'),patch.dict(os.environ,{'MLFLOW_TRACKING_AUTH':'native-test-marker'}), \
             patch.object(mlflow,'get_tracking_uri',return_value='https://native-aidp.invalid'), \
             patch.object(mlflow,'get_registry_uri',return_value='https://native-aidp.invalid'), \
             patch('aidp_lakehouse.training_mlflow._distribution_version',side_effect=version), \
             patch('aidp_lakehouse.training_mlflow.inspect.signature',side_effect=lambda obj:signature if obj is mlflow.sklearn.log_model else inspect.Signature([inspect.Parameter('sparse_output',inspect.Parameter.KEYWORD_ONLY,default=True)])), \
             patch('mlflow.tracking.MlflowClient',return_value=client), \
             patch.object(mlflow,'set_tracking_uri') as set_tracking,patch.object(mlflow,'set_registry_uri') as set_registry:
            caps=preflight(TrainingConfig())
        self.assertIsNone(caps['mlflow_distribution_version'])
        self.assertEqual(caps['native_serialization_default'],'skops')
        client.search_experiments.assert_called_once_with(max_results=1)
        set_tracking.assert_not_called();set_registry.assert_not_called()


    def test_observed_schemeless_native_uri_cannot_fall_back_to_files(self):
        import os,mlflow
        from unittest.mock import patch
        from aidp_lakehouse.training_contract import TrainingConfig
        from aidp_lakehouse.training_mlflow import preflight
        with patch('platform.machine',return_value='x86_64'), \
             patch.dict(os.environ,{'MLFLOW_TRACKING_AUTH':'native-test-marker'}), \
             patch.object(mlflow,'get_tracking_uri',return_value='opaque-native-value'), \
             patch('mlflow.tracking.MlflowClient') as client:
            with self.assertRaisesRegex(RuntimeError,'no local fallback'):preflight(TrainingConfig())
            client.assert_not_called()
