"""Canonical notebook simulations, without Spark/AIDP connections."""
import ast
import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from aidp_lakehouse import bronze_workflow as w
from aidp_lakehouse.bronze_migration import TARGET
from dataclasses import asdict


class WorkflowTests(unittest.TestCase):
    def make_run(self):
        spark=SimpleNamespace(version='simulated',sparkContext=object())
        return w.BronzeRun(spark,asdict(TARGET))

    def test_v1_registration_stops_before_registering_any_entity(self):
        run=self.make_run();run.manifest={}
        class Row:
            def asDict(self): return {'tableName':'orders'}
        run.spark.sql=lambda sql: SimpleNamespace(collect=lambda: [Row()] if sql.startswith('SHOW') else [])
        with patch.object(w,'_details',return_value={'location':'oci://bucket@ns/source-data/v1/orders/initial/'}), \
             patch.object(w,'register_bronze') as register:
            with self.assertRaisesRegex(ValueError,'separate reviewed migration'): run.register()
            register.assert_not_called()
        self.assertTrue(run.failed)

    def test_pass_requires_all_comparisons_and_no_earlier_failure(self):
        run=self.make_run()
        with self.assertRaises(ValueError): run.finish()
        run.verification=[{}]*4
        with self.assertRaises(ValueError): run.finish()
        clean=self.make_run();clean.verification=[{}]*4
        with contextlib.redirect_stdout(io.StringIO()) as out: clean.finish()
        self.assertIn('BRONZE PASS',out.getvalue())

    def test_verification_uses_each_manifest_count_and_failure_prevents_pass(self):
        run=self.make_run()
        run.manifest={'row_counts':{'customers.csv':600,'orders.csv':30600,'deliveries.csv':30000,'events.jsonl':94572}}
        with patch.object(w,'compare_table',side_effect=lambda s,c,e,**kw: {'rows':kw['expected_count']}) as compare:
            run.verify()
            self.assertEqual([r['rows'] for r in run.verification],[600,30600,30000,94572])
            self.assertEqual(compare.call_count,4)
        with patch.object(w,'compare_table',side_effect=ValueError('full-row mismatch')):
            with self.assertRaisesRegex(ValueError,'full-row mismatch'): run.verify()
        with self.assertRaises(ValueError): run.finish()

    def test_notebook_has_one_configuration_and_no_destructive_code(self):
        nb=json.loads((Path(__file__).parents[1]/'notebooks/01_register_bronze.ipynb').read_text())
        code=[''.join(c['source']) for c in nb['cells'] if c['cell_type']=='code']
        self.assertEqual(sum('CONFIG =' in cell for cell in code),1)
        for cell in code:
            ast.parse(cell)
            self.assertFalse(any(word in cell.upper() for word in ['DROP TABLE','ALTER TABLE','CREATE OR REPLACE']))
        self.assertIn('run.setup()',code);self.assertIn('run.controls()',code)
        self.assertIn('run.register()',code);self.assertIn('run.verify()',code)

    def test_entire_notebook_runs_in_a_fresh_simulated_session(self):
        nb=json.loads((Path(__file__).parents[1]/'notebooks/01_register_bronze.ipynb').read_text())
        frame=SimpleNamespace(limit=lambda n:frame,show=lambda **kw:None)
        spark=SimpleNamespace(version='simulated',sparkContext=object(),table=lambda n:frame)
        spark.sql=lambda sql: SimpleNamespace(collect=lambda:[],show=lambda **kw:None)
        manifest={'row_counts':{'customers.csv':600,'orders.csv':30600,'deliveries.csv':30000,'events.jsonl':94572},
                  'batch_id':'initial','extracted_at':'2026-10-28T00:00:00Z'}
        scope={'spark':spark}
        with patch.object(w,'ensure_catalog') as setup, \
             patch.object(w,'read_control_documents',return_value=(manifest,{})), \
             patch.object(w,'register_bronze',return_value=[]), \
             patch.object(w,'compare_table',side_effect=lambda s,c,e,**kw:{'rows':kw['expected_count']}), \
             contextlib.redirect_stdout(io.StringIO()) as out:
            for cell in nb['cells']:
                if cell['cell_type']=='code': exec(''.join(cell['source']),scope)
        setup.assert_called_once()
        self.assertIn('BRONZE PASS',out.getvalue())
        self.assertEqual(len(scope['verification']),4)
