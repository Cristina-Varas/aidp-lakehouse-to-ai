"""Data contracts and SQL semantics, plus simulated native adapter boundaries.

SQLite checks SELECT semantics after replacing only Spark Delta table bindings;
it is not proof of Spark time travel, native authorization, RAG or LLM behavior.
"""
import ast
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import zipfile

from aidp_lakehouse.agent_bundle import bundle_files, write_agent_bundle
from aidp_lakehouse.agent_queries import AgentSnapshot, MODEL, RUN, PREDICTIONS, SCORING, TOOLS, native_sql_config, query_sql
from aidp_lakehouse.agent_readiness import validate_agent_records
from aidp_lakehouse.gold import V2_PREFIX

SNAPSHOT = AgentSnapshot(2, 1)
IDS = {PREDICTIONS: 'prediction-table-id', SCORING: 'scoring-table-id'}


def records():
    accepted = datetime(2026, 10, 22, tzinfo=timezone.utc)
    p, s = [], []
    for i in range(600):
        order = f'O-{30001+i:08d}'
        customer = f'C-{1+i%100:06d}'
        row = dict(order_id=order, customer_id=customer, accepted_at=accepted,
                   promised_delivery_at_acceptance=accepted+timedelta(days=3),
                   priority_at_acceptance='NORMAL', planned_carrier_at_acceptance=['CARRIER_A','CARRIER_B','CARRIER_C'][i%3],
                   customer_region_at_acceptance=['NORTH','SOUTH','EAST','WEST'][i%4],
                   customer_segment_at_acceptance='STANDARD', inventory_fill_rate_at_acceptance=.8,
                   warehouse_load_at_acceptance=.9, _batch_id='initial', _reference_date=date(2026,10,28))
        s.append(row)
        probability = .4 if i<450 else .2
        p.append(dict(order_id=order, customer_id=customer, accepted_at=accepted,
                      late_probability=probability, predicted_late=probability>=.25, decision_threshold=.25,
                      model_name=MODEL, model_version='1', training_run_id=RUN,
                      training_cutoff=datetime(2026,4,1,tzinfo=timezone.utc), input_table=SCORING,
                      input_table_id=IDS[SCORING], input_delta_version=1, input_fingerprint='a'*64,
                      source_prefix=V2_PREFIX, _batch_id='initial', _reference_date=date(2026,10,28)))
    return p, s


def receipt():
    p,s=records()
    return validate_agent_records(p,s,IDS,SNAPSHOT)


class AgentDataTests(unittest.TestCase):
    def test_complete_cohort_recovers_saved_rule_and_records_pending_states(self):
        proof=receipt()
        self.assertEqual((proof['rows'],proof['alerts'],proof['non_alerts']),(600,450,150))
        self.assertEqual(proof['decision_threshold'],.25)
        self.assertEqual(proof['predictions_version'],2)
        self.assertFalse(proof['agent_deployed']);self.assertFalse(proof['rag_verified'])

    def test_missing_duplicate_or_unmatched_keys_are_rejected(self):
        p,s=records()
        for badp,bads in ((p[:-1],s),(p,[s[0]]*600),([p[0]]*600,s)):
            with self.subTest(),self.assertRaises(ValueError):validate_agent_records(badp,bads,IDS,SNAPSHOT)

    def test_wrong_model_snapshot_or_corrupt_scores_are_rejected(self):
        p,s=records()
        for key,value in [('model_version','2'),('model_name','other'),('training_run_id','0'*32),
                          ('input_delta_version',0),('input_table_id','recreated-table'),('source_prefix','v1'),
                          ('late_probability',float('nan')),('late_probability',float('inf')),
                          ('late_probability',1.01),('predicted_late',False),('decision_threshold',float('nan')),
                          ('decision_threshold',.3),('input_fingerprint','bad'),('customer_id','C-999999'),
                          ('training_cutoff',datetime(2026,10,23,tzinfo=timezone.utc))]:
            bad=deepcopy(p);bad[0][key]=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):
                validate_agent_records(bad,s,IDS,SNAPSHOT)

    def test_changed_source_acceptance_or_observation_is_rejected(self):
        p,s=records()
        for key,value in [('accepted_at',datetime(2026,10,21,tzinfo=timezone.utc)),('_reference_date',date(2026,10,29))]:
            bad=deepcopy(s);bad[0][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):validate_agent_records(p,bad,IDS,SNAPSHOT)


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.db.row_factory=sqlite3.Row
        for table,rows in zip(('predictions','scoring'),records()):
            names=list(rows[0]);self.db.execute(f'CREATE TABLE {table} ({", ".join(names)})')
            def encode(v):return v.isoformat() if isinstance(v,(datetime,date)) else v
            self.db.executemany(f'INSERT INTO {table} VALUES ({", ".join("?" for _ in names)})',
                                [tuple(encode(row[n]) for n in names) for row in rows])

    def tearDown(self):self.db.close()

    def execute(self,tool,params):
        sql=query_sql(tool,params,SNAPSHOT)
        sql=sql.replace(f'{PREDICTIONS} VERSION AS OF 2','predictions').replace(f'{SCORING} VERSION AS OF 1','scoring')
        return [dict(r) for r in self.db.execute(sql)]

    def test_summary_groups_and_missing_order(self):
        summary=self.execute('risk_summary',{})[0]
        self.assertEqual((summary['open_orders'],summary['alert_orders'],summary['non_alert_orders']),(600,450,150))
        self.assertEqual(summary['alert_fraction'],.75)
        for tool in ('risk_by_carrier','risk_by_region'):
            groups=self.execute(tool,{})
            self.assertEqual(sum(g['open_orders'] for g in groups),600)
            self.assertEqual(sum(g['alert_orders'] for g in groups),450)
        self.assertEqual(self.execute('get_order_risk',{'order_id':'O-99999999'}),[])

    def test_ranking_ties_lookup_and_customer_use_exact_evidence(self):
        top=self.execute('rank_open_orders',{'limit':3})
        self.assertEqual([r['order_id'] for r in top],['O-00030001','O-00030002','O-00030003'])
        self.assertEqual([r['risk_rank'] for r in top],[1,2,3])
        self.assertEqual(self.execute('get_order_risk',{'order_id':top[0]['order_id']})[0]['late_probability'],.4)
        customer=self.execute('get_customer_orders',{'customer_id':'C-000001'})
        self.assertEqual(len(customer),6);self.assertTrue(all(r['total_matching']==6 for r in customer))

    def test_sql_injection_and_unknown_arguments_never_reach_a_query(self):
        for tool,args in [('get_order_risk',{'order_id':"O-00030001' OR 1=1 --"}),
                          ('get_customer_orders',{'customer_id':"C-000001'; DROP TABLE x"}),
                          ('get_order_risk',{}),('risk_summary',{'query':'SELECT secret'}),
                          ('rank_open_orders',{'limit':'10'}),('rank_open_orders',{'limit':True}),
                          ('rank_open_orders',{'limit':1.2}),('rank_open_orders',{'limit':21}),
                          ('rank_open_orders',{'limit':float('nan')}),('unknown',{})]:
            with self.subTest(tool=tool,args=args),self.assertRaises(ValueError):query_sql(tool,args,SNAPSHOT)
        self.assertIn('LIMIT 10',query_sql('rank_open_orders',{'limit':10.0},SNAPSHOT))

    def test_fixed_versions_and_private_native_connection_are_required(self):
        for value in (-1,True,'latest',1.5):
            with self.assertRaises(ValueError):AgentSnapshot(value,1)
        with self.assertRaises(ValueError):native_sql_config({},'risk_summary',{},SNAPSHOT)
        connection={'catalogKey':'lakehouse_demo','schemaKey':'gold','verifiedClusterField':'native-id','query':'OLD'}
        conf=native_sql_config(connection,'risk_summary',{},SNAPSHOT)
        self.assertEqual(conf['verifiedClusterField'],'native-id');self.assertEqual(conf['maxRows'],1)
        self.assertEqual(connection['query'],'OLD')
        self.assertIn('VERSION AS OF 2',conf['query']);self.assertIn('VERSION AS OF 1',conf['query'])
        self.assertNotIn('{{',conf['query'])

    def test_native_ui_json_configuration_preserves_target_and_parameter_checks(self):
        connection = {'queryType': 'SPARK', 'catalogKey': 'lakehouse_demo',
                      'schemaKey': 'gold', 'sparkComputeKey': 'verified-cluster'}
        self.assertEqual(native_sql_config(json.dumps(connection), 'risk_summary', {}, SNAPSHOT),
                         native_sql_config(connection, 'risk_summary', {}, SNAPSHOT))
        self.assertEqual(native_sql_config(connection, 'risk_summary', {}, SNAPSHOT)['catalogType'],
                         'STANDARD')
        self.assertEqual(native_sql_config(connection, 'risk_summary', {}, SNAPSHOT)['clusterKey'],
                         'verified-cluster')
        with self.assertRaises(ValueError):
            native_sql_config(dict(connection, clusterKey='different-cluster'), 'risk_summary', {}, SNAPSHOT)
        for key in ('catalogType', 'catalog_type'):
            with self.assertRaises(ValueError):
                native_sql_config(dict(connection, **{key:'EXTERNAL'}), 'risk_summary', {}, SNAPSHOT)
        invalid = ['not json', 'null', '[]', '1', '"text"', '{}',
                   '{"catalogKey":"other","catalogKey":"lakehouse_demo","schemaKey":"gold"}',
                   json.dumps(dict(connection, catalogKey='other')),
                   json.dumps(dict(connection, password='must-not-be-used'))]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                native_sql_config(value, 'risk_summary', {}, SNAPSHOT)
        with self.assertRaises(ValueError):
            native_sql_config(json.dumps(connection), 'get_order_risk',
                              {'order_id': "O-00030255' OR 1=1 --"}, SNAPSHOT)


class BundleTests(unittest.TestCase):
    def test_deterministic_bundle_and_document_only_ingestion_folder(self):
        output=bundle_files(receipt());self.assertEqual(output,bundle_files(receipt()))
        self.assertEqual(sorted(k for k in output if k.startswith('documents/')),
                         ['documents/incident_escalation.txt','documents/order_prioritization.txt'])
        root=Path(__file__).parents[1]/'src/aidp_lakehouse'
        for name in ('incident_escalation','order_prioritization'):
            self.assertEqual(output[f'documents/{name}.txt'],(root/'policies'/f'{name}.md').read_bytes())
        manifest=json.loads(output['manifest.json'])
        for name,entry in manifest['files'].items():
            self.assertEqual(entry['sha256'],hashlib.sha256(output[name]).hexdigest())
        archive=zipfile.ZipFile(io.BytesIO(output['operations_sql_tools.zip']))
        self.assertEqual(set(archive.namelist()),{'agent_queries.py','tool_config.json','tool_implementation.py'})
        config=json.loads(archive.read('tool_config.json'))
        self.assertEqual(len(config['tools']),6)
        self.assertTrue(all(t['conf']['sql_connection']=={} for t in config['tools']))

    def test_identical_rerun_and_conflicting_output_preservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)/'bundle'
            self.assertGreater(write_agent_bundle(directory,receipt())['changed_files'],0)
            self.assertEqual(write_agent_bundle(directory,receipt())['changed_files'],0)
            (directory/'agent_instructions.txt').write_text('custom local edits')
            with self.assertRaises(ValueError):write_agent_bundle(directory,receipt())
            self.assertEqual((directory/'agent_instructions.txt').read_text(),'custom local edits')

    def test_native_adapter_simulation_validates_before_calling_tool(self):
        archive=zipfile.ZipFile(io.BytesIO(bundle_files(receipt())['operations_sql_tools.zip']))
        code=archive.read('tool_implementation.py').decode()
        registered=[]
        class CustomBase:
            @classmethod
            def register(cls,subclass):registered.append(subclass);return subclass
        class ToolConf:
            def __init__(self,**kwargs):self.value=kwargs
            def model_dump(self):return self.value
        factory=Mock(return_value=SimpleNamespace(invoke=Mock(return_value={'rows':[{'open_orders':600}]})))
        modules={}
        for name in ['aidputils','aidputils.agents','aidputils.agents.tools',
                     'aidputils.agents.tools.custom_tools','aidputils.agents.tools.custom_tools.base',
                     'aidputils.agents.toolkit','aidputils.agents.toolkit.configs','aidputils.agents.toolkit.tool_helper']:
            modules[name]=ModuleType(name)
        modules['aidputils.agents.tools.custom_tools.base'].CustomToolBase=CustomBase
        modules['aidputils.agents.toolkit.configs'].AIDPToolConf=ToolConf
        modules['aidputils.agents.toolkit.tool_helper'].create_langgraph_tool=factory
        import aidp_lakehouse.agent_queries as queries
        modules['agent_queries']=queries
        scope={}
        with patch.dict(sys.modules,modules):exec(compile(code,'tool_implementation.py','exec'),scope)
        self.assertEqual(len(registered),6)
        config=json.loads(archive.read('tool_config.json'))['tools'][0]['conf']
        config['predictions_version']='2'
        config['sql_connection']={'catalogKey':'lakehouse_demo','schemaKey':'gold','verifiedClusterField':'native-id'}
        for bad in [{'order_id':"O-00030001' OR 1=1 --"},{'order_id':'O-00030001','query':'x'}]:
            with self.assertRaises(ValueError):scope['GetOrderRisk']._execute_tool({'conf':config},bad)
        factory.assert_not_called()
        answer=scope['RiskSummary']._execute_tool({'conf':config},{})
        self.assertEqual(answer['predictions_version'],2)
        conf=factory.call_args.args[0]
        self.assertEqual(conf['params'],[]);self.assertIn('VERSION AS OF 2',conf['conf']['query'])
        factory.return_value.invoke.return_value={'success':False,'error':'unavailable'}
        with self.assertRaises(RuntimeError):scope['RiskSummary']._execute_tool(config,{})
        factory.return_value.invoke.return_value=json.dumps({'isError':True,'error':{
            'aidp_error_code':'AIDP_SQL_TOOL_INVALID_QUERY_ERROR',
            'data':{'details':{'reason':'SQL_SCHEMA_VALIDATION_UNSUPPORTED_QUALIFIER'}}}})
        with self.assertRaises(RuntimeError):scope['RiskSummary']._execute_tool(config,{})

    def test_notebook_is_clean_read_only_and_cannot_claim_deployment(self):
        notebook=json.loads((Path(__file__).parents[1]/'notebooks/06_prepare_operations_agent.ipynb').read_text())
        codes=[''.join(c['source']) for c in notebook['cells'] if c['cell_type']=='code']
        self.assertEqual(sum('CONFIG =' in c for c in codes),1)
        for c in notebook['cells']:
            if c['cell_type']=='code':
                ast.parse(''.join(c['source']));self.assertEqual(c['outputs'],[]);self.assertIsNone(c['execution_count'])
        self.assertIn('sql_verification = bundle_result = None',codes[2])
        with self.assertRaises(RuntimeError):exec(codes[-1],{'receipt':None,'sql_verification':None})
        self.assertIn('Native tools, RAG and agent acceptance pending.',codes[-1])
