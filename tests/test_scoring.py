"""Real sklearn inference fixtures and saved-contract rejection tests; no AIDP."""
from copy import deepcopy
from dataclasses import replace
from importlib.util import find_spec
import json
from pathlib import Path
import unittest
from unittest.mock import Mock
from aidp_lakehouse.scoring import ScoringConfig,MODEL_NAME,TRAINING_RUN

HAS_ML=all(find_spec(n) is not None for n in ('numpy','pandas','sklearn'))


def open_fixture():
    import pandas as pd
    from test_training import fixture
    from aidp_lakehouse.gold import CONTRACTS_GOLD
    template=fixture().iloc[:1].to_dict('records')[0]
    rows=[]
    for i in range(600):
        row=dict(template,order_id=f'OPEN-{i:04d}',accepted_at=pd.Timestamp('2026-10-22T00:00:00Z'),
                 promised_delivery_at_acceptance=pd.Timestamp('2026-10-27T00:00:00Z'))
        row['transit_distance_km']=100+i
        rows.append({n:row[n] for n,_ in CONTRACTS_GOLD['late_delivery_scoring']})
    return pd.DataFrame.from_records(rows)


def saved_fixture(config=None):
    """Local fitted fixture with the same artifacts as the real training publisher."""
    from aidp_lakehouse.training import make_pipeline,model_input
    from aidp_lakehouse.training_contract import feature_contract
    from aidp_lakehouse.gold import CONTRACTS_GOLD
    from test_training import fixture
    cfg=config or ScoringConfig()
    train=fixture();pipeline=make_pipeline({'kind':'logistic','C':1},42,threshold=.25)
    pipeline.fit(model_input(train),train['is_late'].astype('int64'))
    cutoff='2026-04-01T00:00:00Z'
    registry={'run_id':cfg.training_run_id,'registered_model':cfg.registered_model_name,'version':cfg.model_version,
              'model_uri':cfg.model_uri,'registry_load_verified':True,'threshold':.25,'training_cutoff':cutoff}
    source={'reference_date':'2026-10-28','prefix':cfg.gold.bronze.prefix,'predictors':feature_contract()['raw_predictors'],
            'open_rows':600}
    inputs={n:{'table':f'{cfg.gold.gold_namespace}.{n}','delta_version':0,'schema':schema}
            for n,schema in CONTRACTS_GOLD.items() if n.startswith('late_delivery_')}
    inputs['source_contract']=source
    report={'feature_contract':feature_contract(),'selected_threshold':.25,'final_training_cutoff':cutoff,
            'partitions':{'boundaries':{'final_training_cutoff':cutoff},'final_train':{
                'last_outcome_available_at':'2026-03-31T23:00:00Z','last_acceptance':'2026-03-30T00:00:00Z'}},
            'test_used_for_training':False}
    artifacts={'feature_contract.json':feature_contract(),'evaluation_report.json':report,
               'registry_verification.json':registry,'input_delta_versions.json':inputs}
    params={'selected_threshold':'.25','training_cutoff':cutoff,'test_used_for_training':'False'}
    snapshot={'table':cfg.input_table,'table_id':'local-fixture-table-id','delta_version':0}
    return pipeline,artifacts,params,cfg,snapshot


class ScoringNotebookTests(unittest.TestCase):
    def test_exact_version_identity_and_no_aliases(self):
        cfg=ScoringConfig()
        self.assertEqual(cfg.model_uri,f'models:/{MODEL_NAME}/1')
        self.assertEqual(cfg.training_run_id,TRAINING_RUN)
        for version in ('latest','Production','@champion','0','1/extra'):
            with self.subTest(version=version),self.assertRaises(ValueError):ScoringConfig(model_version=version)
        with self.assertRaises(ValueError):ScoringConfig(input_delta_version=-1)

    def test_clean_notebook_single_config_and_pass_gate(self):
        import ast
        notebook=json.loads((Path(__file__).parents[1]/'notebooks/05_score_open_orders.ipynb').read_text())
        codes=[''.join(c['source']) for c in notebook['cells'] if c['cell_type']=='code']
        self.assertEqual(sum('CONFIG =' in c for c in codes),1)
        scope={};exec(codes[0],scope)
        self.assertNotIn('threshold',scope['CONFIG']);self.assertNotIn('cutoff',scope['CONFIG'])
        self.assertEqual(scope['CONFIG']['model_version'],'1')
        all_code='\n'.join(codes)
        for forbidden in ('.fit(', 'register_model(', 'set_tracking_uri(', 'set_registry_uri('):
            self.assertNotIn(forbidden,all_code)
        self.assertIn('require_aidp=True',all_code)
        for cell in notebook['cells']:
            if cell['cell_type']=='code':ast.parse(''.join(cell['source']));self.assertEqual(cell['outputs'],[]);self.assertIsNone(cell['execution_count'])
        with self.assertRaises(RuntimeError):exec(codes[-1],{'verification':None})


    def test_repeat_failure_never_returns_pass(self):
        from unittest.mock import patch
        from aidp_lakehouse.scoring_spark import score_publish_repeat
        first=Mock()
        with patch('aidp_lakehouse.scoring_spark.score_orders',side_effect=[('values',{'rows':600}),RuntimeError('second inference failed')]), \
             patch('aidp_lakehouse.scoring_spark.prediction_frame',return_value=first), \
             patch('aidp_lakehouse.scoring_spark.publish_predictions',return_value={'rows':600,'inserted_rows':600}):
            with self.assertRaisesRegex(RuntimeError,'second inference failed'):
                score_publish_repeat(None,None,None,None,None)
        first.cache.return_value.unpersist.assert_called_once()


@unittest.skipUnless(HAS_ML,'Requires optional sklearn training environment')
class ScoringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.saved=saved_fixture()

    def bundle(self):
        from aidp_lakehouse.scoring import verify_pipeline
        pipeline,artifacts,params,cfg,snapshot=self.saved
        return verify_pipeline(pipeline,deepcopy(artifacts),params,cfg)

    def test_saved_pipeline_inference_repeat_and_no_fit(self):
        import numpy as np
        import pandas as pd
        from aidp_lakehouse.scoring import score_orders
        from unittest.mock import patch
        bundle=self.bundle();cfg=self.saved[3];snapshot=self.saved[4]
        data=open_fixture()
        with patch.object(bundle.pipeline,'fit',side_effect=AssertionError('No training allowed')):
            first,summary=score_orders(data,bundle,cfg,snapshot)
            second,repeated=score_orders(data.sample(frac=1,random_state=7),bundle,cfg,snapshot)
        pd.testing.assert_frame_equal(first,second);self.assertEqual(summary,repeated)
        self.assertEqual(len(first),600)
        np.testing.assert_array_equal(first['predicted_late'],first['late_probability']>=bundle.threshold)
        self.assertEqual(summary['classes'],[0,1]);self.assertEqual(summary['late_probability_column'],1)

    def test_artifact_conflicts_fail_closed(self):
        from aidp_lakehouse.scoring import verify_pipeline
        pipeline,artifacts,params,cfg,_=self.saved
        def mutate_threshold(a):a['registry_verification.json']['threshold']=.5
        def mutate_cutoff(a):a['registry_verification.json']['training_cutoff']='2026-10-27T00:00:00Z'
        def mutate_features(a):a['feature_contract.json']['raw_predictors'].append('current_status')
        def mutate_run(a):a['registry_verification.json']['run_id']='a'*32
        def mutate_future_label(a):a['evaluation_report.json']['partitions']['final_train']['last_outcome_available_at']='2026-04-01T00:00:00Z'
        for mutate in (mutate_threshold,mutate_cutoff,mutate_features,mutate_run,mutate_future_label):
            wrong=deepcopy(artifacts);mutate(wrong)
            with self.subTest(mutation=mutate.__name__),self.assertRaises(ValueError):verify_pipeline(pipeline,wrong,params,cfg)

    def test_pipeline_preprocessing_contract_is_checked(self):
        from aidp_lakehouse.scoring import verify_pipeline
        pipeline,artifacts,params,cfg,_=self.saved
        wrong=deepcopy(pipeline)
        wrong.named_steps['preprocessing'].transformers[0][2].append('current_priority')
        with self.assertRaisesRegex(ValueError,'preprocessing columns'):
            verify_pipeline(wrong,artifacts,params,cfg)

    def test_invalid_open_inputs_fail_before_inference(self):
        from aidp_lakehouse.scoring import score_orders
        from unittest.mock import patch
        bundle=self.bundle();cfg=self.saved[3];snapshot=self.saved[4]
        for mode in ('duplicate','partial','before_cutoff','future_acceptance','invalid_promise','wrong_reference','nan','metadata_feature','numeric_range','categorical_domain'):
            data=open_fixture()
            if mode=='duplicate':data.loc[0,'order_id']=data.loc[1,'order_id']
            elif mode=='partial':data=data.iloc[:-1]
            elif mode=='before_cutoff':data.loc[0,'accepted_at']='2026-03-31T23:59:59Z'
            elif mode=='future_acceptance':data.loc[0,'accepted_at']='2026-10-29T00:00:00Z'
            elif mode=='invalid_promise':data.loc[0,'promised_delivery_at_acceptance']=data.loc[0,'accepted_at']
            elif mode=='wrong_reference':data.loc[0,'_batch_id']='incremental'
            elif mode=='nan':data.loc[0,'transit_distance_km']=float('nan')
            elif mode=='metadata_feature':data['outcome_available_at']='2026-10-28'
            elif mode=='numeric_range':data.loc[0,'transit_distance_km']=9999
            elif mode=='categorical_domain':data.loc[0,'planned_carrier_at_acceptance']='UNKNOWN'
            with self.subTest(mode=mode),patch.object(bundle.pipeline,'predict_proba',side_effect=AssertionError('Reject before inference')):
                with self.assertRaises(ValueError):score_orders(data,bundle,cfg,snapshot)

    def test_probability_class_order_and_bad_outputs(self):
        import numpy as np
        from dataclasses import replace
        from aidp_lakehouse.scoring import score_orders
        bundle=self.bundle();cfg=self.saved[3];snapshot=self.saved[4]
        # Simulated response order verifies positive-class lookup, not a fixed [:,1].
        pipeline=Mock(classes_=np.array([1,0]))
        pipeline.predict_proba.return_value=np.tile([.75,.25],(600,1))
        pipeline.predict.return_value=np.ones(600,dtype=int)
        reversed_bundle=replace(bundle,pipeline=pipeline)
        result,summary=score_orders(open_fixture(),reversed_bundle,cfg,snapshot)
        self.assertEqual(summary['late_probability_column'],0);self.assertTrue((result['late_probability']==.75).all())
        for probabilities in ([float('nan'),.25],[1.1,-.1],[.6,.6]):
            pipeline.predict_proba.return_value=np.tile(probabilities,(600,1))
            with self.assertRaises(ValueError):score_orders(open_fixture(),reversed_bundle,cfg,snapshot)
        pipeline.predict_proba.return_value=np.tile([.75,.25],(600,1));pipeline.predict.return_value=np.zeros(600,dtype=int)
        with self.assertRaisesRegex(ValueError,'threshold'):score_orders(open_fixture(),reversed_bundle,cfg,snapshot)
        pipeline.predict_proba.return_value=np.tile([.25,.75],(600,1));pipeline.predict.return_value=np.ones(600,dtype=int)
        # Equality with threshold must classify as late.
        result,_=score_orders(open_fixture(),reversed_bundle,cfg,snapshot)
        self.assertTrue(result['predicted_late'].all())
