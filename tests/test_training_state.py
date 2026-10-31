"""State/contract/notebook regression tests, no ML libraries or cloud needed."""
from copy import deepcopy
from dataclasses import asdict
import ast
import json
from pathlib import Path
import unittest
from unittest.mock import Mock
from aidp_lakehouse.gold import CONTRACTS_GOLD,PREDICTORS
from aidp_lakehouse.gold_workflow import GoldVerification
from aidp_lakehouse.training_contract import TrainingConfig,feature_contract


def complete():
    return {'validation':{'reconciliation':'PASS'},'fingerprints':{n:{'rows':1} for n in CONTRACTS_GOLD},
            'publication':{n:{'inserted_rows':0} for n in CONTRACTS_GOLD}}


class GoldProofTests(unittest.TestCase):
    def test_skipping_first_or_second_cannot_pass(self):
        state=GoldVerification()
        with self.assertRaises(ValueError):state.require_pass()
        with self.assertRaises(ValueError):state.build_second(None,None,lambda *_:complete())
        state.build_first(None,None,lambda *_:complete())
        with self.assertRaises(ValueError):state.require_pass()
        state.build_second(None,None,lambda *_:complete());self.assertTrue(state.require_pass())

    def test_restart_failure_and_interrupted_rerun_invalidate_previous_success(self):
        state=GoldVerification()
        state.build_first(None,None,lambda *_:complete());state.build_second(None,None,lambda *_:complete())
        with self.assertRaises(RuntimeError):state.build_first(None,None,Mock(side_effect=RuntimeError('failed')))
        with self.assertRaises(ValueError):state.require_pass()
        state.build_first(None,None,lambda *_:complete());state.build_second(None,None,lambda *_:complete())
        with self.assertRaises(KeyboardInterrupt):state.build_second(None,None,Mock(side_effect=KeyboardInterrupt()))
        with self.assertRaises(ValueError):state.require_pass()

    def test_changed_fingerprints_or_new_insertions_cannot_pass(self):
        for mode in ('hash','insert'):
            state=GoldVerification();state.build_first(None,None,lambda *_:complete())
            wrong=complete()
            if mode=='hash':wrong['fingerprints']['order_operations']['rows']=2
            else:wrong['publication']['order_operations']['inserted_rows']=1
            with self.assertRaises(ValueError):state.build_second(None,None,lambda *_:wrong)
            with self.assertRaises(ValueError):state.require_pass()

    def test_notebook_final_cell_requires_gate_and_runtime_restart_resets_it(self):
        import contextlib,io
        notebook=json.loads((Path(__file__).parents[1]/'notebooks/03_build_gold.ipynb').read_text())
        code=[''.join(c['source']) for c in notebook['cells'] if c['cell_type']=='code']
        self.assertIn('gold_verification = None',code[0])
        final=code[-1];self.assertIn('gold_verification.require_pass()',final)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(ValueError):exec(final,{'gold_verification':GoldVerification()})
        self.assertNotIn('GOLD PASS:',out.getvalue())


class TrainingNotebookTests(unittest.TestCase):
    def test_configuration_and_feature_contract(self):
        cfg=TrainingConfig()
        self.assertEqual(cfg.test_start,'2026-04-01T00:00:00Z')
        self.assertEqual(feature_contract()['raw_predictors'],list(PREDICTORS))
        with self.assertRaises(ValueError):TrainingConfig(validation_start=cfg.test_start)
        with self.assertRaises(ValueError):TrainingConfig(test_start='2026-11-01T00:00:00Z')

    def test_clean_notebook_single_configuration_no_scoring(self):
        import nbformat
        nb=nbformat.read(Path(__file__).parents[1]/'notebooks/04_train_late_delivery.ipynb',as_version=4)
        nbformat.validate(nb)
        codes=[c.source for c in nb.cells if c.cell_type=='code']
        self.assertEqual(sum('CONFIG =' in c for c in codes),1)
        scope={};exec(codes[0],scope)
        self.assertEqual(scope['CONFIG']['source']['reference_date'],'2026-10-28')
        for c in nb.cells:
            if c.cell_type=='code':ast.parse(c.source);self.assertEqual(c.outputs,[]);self.assertIsNone(c.execution_count)
        self.assertIn('require_aidp=True','\n'.join(codes))
        self.assertNotIn('set_tracking_uri','\n'.join(codes))
        self.assertLess(next(i for i,c in enumerate(codes) if 'capabilities = preflight' in c),
                        next(i for i,c in enumerate(codes) if 'result, registry = run_experiment' in c))
