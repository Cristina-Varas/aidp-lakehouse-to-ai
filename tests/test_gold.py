"""Gold contracts/notebook checks, without Spark or AIDP."""
import ast
from dataclasses import asdict
import json
from pathlib import Path
import unittest
from aidp_lakehouse.gold import (GoldConfig, CONTRACTS_GOLD, IDENTIFIERS, PREDICTORS,
                                 LABEL, TRAINING_METADATA, SCORING_METADATA, VERSION)
from aidp_lakehouse.aidp_bronze import BronzeConfig
from aidp_lakehouse.generator import FEATURES


class GoldContractTests(unittest.TestCase):
    def test_exact_original_acceptance_allowlist_and_roles(self):
        self.assertEqual(list(PREDICTORS),FEATURES)
        for table,metadata in [('late_delivery_training',(LABEL,*TRAINING_METADATA)),
                               ('late_delivery_scoring',SCORING_METADATA)]:
            names=[n for n,_ in CONTRACTS_GOLD[table]]
            self.assertEqual(names,list((*IDENTIFIERS,*PREDICTORS,*metadata)))
            self.assertEqual(len(names),len(set(names)))
            self.assertFalse(set(PREDICTORS)&{'current_status','current_priority','event_at','event_type',
                                             'delivered_at','actual_carrier','source_updated_at','row_version','order_id'})
        scoring=dict(CONTRACTS_GOLD['late_delivery_scoring'])
        self.assertNotIn('is_late',scoring);self.assertNotIn('probability',scoring)
        self.assertNotIn('outcome_available_at',scoring)

    def test_fixed_initial_v2_configuration(self):
        self.assertIn('/v2/',GoldConfig().bronze.prefix)
        self.assertEqual(GoldConfig().bronze.reference_date,'2026-10-28')
        with self.assertRaises(ValueError):GoldConfig(BronzeConfig())
        with self.assertRaises(ValueError):GoldConfig(BronzeConfig(prefix=GoldConfig().bronze.prefix,seed=7))

    def test_notebook_is_clean_standalone_with_one_config_and_rerun_check(self):
        import nbformat
        path=Path(__file__).parents[1]/'notebooks/03_build_gold.ipynb'
        notebook=nbformat.read(path,as_version=4);nbformat.validate(notebook)
        code=[c.source for c in notebook.cells if c.cell_type=='code']
        for c in notebook.cells:
            if c.cell_type=='code':
                ast.parse(c.source);self.assertEqual(c.outputs,[]);self.assertIsNone(c.execution_count)
        scope={};exec(code[0],scope)
        self.assertEqual(scope['CONFIG'],asdict(GoldConfig().bronze))
        self.assertEqual(sum('CONFIG =' in c for c in code),1)
        all_code='\n'.join(code)
        self.assertIn('build_initial_gold',all_code);self.assertIn('second',all_code)
        self.assertIn('gold_verification.build_second',all_code);self.assertIn('GOLD PASS',all_code)
        self.assertNotIn('SELECT *',all_code.upper())
        self.assertIn('user-reported',notebook.metadata.aidp.runtime_verification)

    def test_distributed_transforms_do_not_collect_business_rows(self):
        source=(Path(__file__).parents[1]/'src/aidp_lakehouse/gold_spark.py').read_text()
        tree=ast.parse(source)
        transform=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='transform_frames')
        calls=[n for n in ast.walk(transform) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)]
        self.assertFalse(any(n.func.attr in ('collect','toPandas','toLocalIterator') for n in calls))
        self.assertIn('groupBy',ast.get_source_segment(source,transform))
        self.assertIn('whenNotMatchedInsertAll',source)
        for unsafe in ('whenMatchedUpdate','whenMatchedDelete','.mode("overwrite")','CREATE OR REPLACE'):
            self.assertNotIn(unsafe,source)

    def test_clean_notebook_runs_top_to_bottom_in_a_fresh_simulated_session(self):
        import contextlib,io
        from types import SimpleNamespace
        from unittest.mock import patch
        notebook=json.loads((Path(__file__).parents[1]/'notebooks/03_build_gold.ipynb').read_text())
        frame=SimpleNamespace(orderBy=lambda *a:frame,limit=lambda n:frame,show=lambda **kw:None)
        spark=SimpleNamespace(version='simulated',conf=SimpleNamespace(set=lambda *a:None),table=lambda n:frame)
        result={'validation':{'reconciliation':'PASS'},'fingerprints':{name:'same' for name in CONTRACTS_GOLD},
                'publication':{n:{'inserted_rows':0} for n in CONTRACTS_GOLD}}
        scope={'spark':spark}
        with patch('aidp_lakehouse.gold_spark.build_initial_gold',return_value=result) as build, \
             contextlib.redirect_stdout(io.StringIO()) as out:
            for c in notebook['cells']:
                if c['cell_type']=='code':exec(''.join(c['source']),scope)
        self.assertEqual(build.call_count,2)
        self.assertIn('GOLD PASS',out.getvalue())
