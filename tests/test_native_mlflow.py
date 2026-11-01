"""Simulated native binding; never signs requests, authenticates, fits or scores."""
from contextlib import ExitStack
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock,patch
from aidp_lakehouse.native_mlflow import NativeBackendConfig,uri_shape

BASE='https://datalake.eu-frankfurt-1.oci.oraclecloud.com/20260430/aiDataPlatforms/ocid1.aidataplatform.test'


def binding():return NativeBackendConfig(BASE+'/workspaces/verified_workspace/mlops',BASE+'/mlops')


class NativeBindingTests(unittest.TestCase):
    def test_reject_local_placeholder_wrong_scope_and_untrusted_host(self):
        for uri in ('mlruns','aidp','mlops','file:///tmp/mlruns','http://localhost/mlops',
                    'https://untrusted.invalid/mlops',BASE+'/mlops?token=forbidden',BASE.replace('https://','https://user:secret@')+'/mlops'):
            with self.subTest(uri_shape=uri_shape(uri)),self.assertRaises(ValueError):
                NativeBackendConfig(uri,BASE+'/mlops')
        with self.assertRaises(ValueError):NativeBackendConfig(BASE+'/workspaces/x/mlops',BASE.replace('.test','.other')+'/mlops')
        with self.assertRaises(ValueError):NativeBackendConfig(BASE+'/mlops',BASE+'/mlops')
        self.assertEqual(uri_shape('unbound_relative_token'),{'configured':True,'scheme':'','has_host':False})
        self.assertNotIn('secret',str(uri_shape('https://user:secret@host/path?token=hidden')))

    def environment(self):
        from aidp_lakehouse.scoring import ScoringConfig
        from aidp_lakehouse.native_mlflow import NATIVE_ENTRY_POINT
        cfg=ScoringConfig()
        model=NS(name=cfg.registered_model_name,version='1',run_id=cfg.training_run_id,status='READY')
        run=NS(info=NS(run_id=cfg.training_run_id,experiment_id='1',status='FINISHED'))
        experiment=NS(experiment_id='1',name='lakehouse_to_ai_late_delivery')
        client=Mock()
        client.get_experiment.return_value=experiment;client.get_run.return_value=run;client.get_model_version.return_value=model
        module=NS(__version__='3.16.0',get_tracking_uri=Mock(return_value='opaque-native-value'),
                  get_registry_uri=Mock(return_value='opaque-native-value'),active_run=Mock(return_value=None),
                  set_tracking_uri=Mock(),set_registry_uri=Mock())
        stack=ExitStack()
        stack.enter_context(patch.dict('sys.modules',{'mlflow':module,'mlflow.tracking':NS(MlflowClient=Mock(return_value=client))}))
        stack.enter_context(patch.dict('os.environ',{'MLFLOW_TRACKING_AUTH':'mlops_auth_provider'}))
        stack.enter_context(patch('aidp_lakehouse.native_mlflow.metadata.entry_points',return_value=[NS(name='mlops_auth_provider',value=NATIVE_ENTRY_POINT)]))
        return stack,module,client,cfg

    def test_verified_reads_precede_global_setters_and_never_score(self):
        from aidp_lakehouse.native_mlflow import initialize_native_backend
        stack,module,client,cfg=self.environment()
        events=[]
        client.get_model_version.side_effect=lambda *a:(events.append('model_read') or client.get_model_version.return_value)
        module.set_tracking_uri.side_effect=lambda *a:events.append('tracking_set')
        module.set_registry_uri.side_effect=lambda *a:events.append('registry_set')
        with stack:result=initialize_native_backend(binding(),cfg)
        self.assertEqual(result['status'],'NATIVE CONNECTION PASS')
        self.assertEqual(events,['model_read','tracking_set','registry_set','model_read'])
        self.assertEqual(client.get_run.call_count,2)
        self.assertFalse(result['model_loaded']);self.assertFalse(result['scoring_performed']);self.assertFalse(result['mlflow_writes'])
        client.predict.assert_not_called();client.create_run.assert_not_called()

    def test_native_authorization_failure_does_not_change_routing_or_leak_error(self):
        from aidp_lakehouse.native_mlflow import initialize_native_backend
        stack,module,client,cfg=self.environment()
        error=RuntimeError('sensitive URL/token must never appear')
        error.json={'code':'NotAuthorizedOrNotFound'}
        client.get_experiment.side_effect=error
        with stack,self.assertRaisesRegex(RuntimeError,'NotAuthorizedOrNotFound') as raised:
            initialize_native_backend(binding(),cfg)
        self.assertNotIn('sensitive',str(raised.exception))
        module.set_tracking_uri.assert_not_called();module.set_registry_uri.assert_not_called()

    def test_wrong_identity_does_not_change_global_configuration(self):
        from aidp_lakehouse.native_mlflow import initialize_native_backend
        stack,module,client,cfg=self.environment();client.get_model_version.return_value.run_id='a'*32
        with stack,self.assertRaises(RuntimeError):initialize_native_backend(binding(),cfg)
        module.set_tracking_uri.assert_not_called();module.set_registry_uri.assert_not_called()

    def test_default_client_failure_restores_previous_routing(self):
        from aidp_lakehouse.native_mlflow import initialize_native_backend
        stack,module,client,cfg=self.environment()
        original=client.get_run.return_value
        client.get_run.side_effect=[original,RuntimeError('redacted backend error')]
        with stack,self.assertRaisesRegex(RuntimeError,'previous URI settings restored'):
            initialize_native_backend(binding(),cfg)
        self.assertEqual(module.set_tracking_uri.call_args.args,('opaque-native-value',))
        self.assertEqual(module.set_registry_uri.call_args.args,('opaque-native-value',))

    def test_missing_native_provider_or_active_run_never_uses_a_replacement(self):
        from aidp_lakehouse.native_mlflow import initialize_native_backend
        stack,module,client,cfg=self.environment()
        with stack,patch.dict('os.environ',{'MLFLOW_TRACKING_AUTH':'other'}),self.assertRaises(RuntimeError):
            initialize_native_backend(binding(),cfg)
        client.get_experiment.assert_not_called()
        stack,module,client,cfg=self.environment();module.active_run.return_value=object()
        with stack,self.assertRaises(RuntimeError):initialize_native_backend(binding(),cfg)
        client.get_experiment.assert_not_called()

    def test_working_platform_binding_is_preserved_without_any_setter(self):
        from aidp_lakehouse.native_mlflow import initialize_native_backend
        stack,module,client,cfg=self.environment()
        module.get_tracking_uri.return_value='https://native-internal.invalid/workspace/mlops'
        module.get_registry_uri.return_value='https://native-internal.invalid/mlops'
        with stack:result=initialize_native_backend(binding(),cfg)
        self.assertEqual(result['route_source'],'existing_verified_native_binding')
        module.set_tracking_uri.assert_not_called();module.set_registry_uri.assert_not_called()
