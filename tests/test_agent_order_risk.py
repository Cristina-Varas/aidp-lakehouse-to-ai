"""Role contract/SQL fixtures; SQLite adaptation is not native Spark evidence."""
from copy import deepcopy
from datetime import datetime
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest

from aidp_lakehouse.agent_order_risk import (TrainingFeatureEvidence, order_risk_response_query,
    apply_order_risk_contract, validate_order_risk_response, PREDICTION_FIELDS)
from aidp_lakehouse.agent_native_sql import native_tools, write_native_sql_bundle
from aidp_lakehouse.agent_queries import AgentSnapshot, MODEL, RUN, PREDICTIONS, SCORING
from aidp_lakehouse.training_contract import feature_contract
import test_operations_agent as fixtures
from test_agent_native_sql import environment


def evidence():
    raw=json.dumps(feature_contract()).encode()
    return TrainingFeatureEvidence.from_native_records(raw,
        {'name':MODEL,'version':'1','run_id':RUN}, {'run_id':RUN,'status':'FINISHED'})


class OrderRiskContractTests(unittest.TestCase):
    def test_complete_export_requires_saved_artifact_and_records_its_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/'complete'
            with self.assertRaises(ValueError):
                write_native_sql_bundle(target,fixtures.receipt(),environment(),'complete')
            self.assertFalse(target.exists())
            write_native_sql_bundle(target,fixtures.receipt(),environment(),'complete',evidence())
            manifest=json.loads((target/'manifest.json').read_text())
            self.assertEqual(manifest['order_risk_response_contract']['sha256'],evidence().sha256)
            self.assertFalse(manifest['runtime_verified'])
            lookup=next(t for t in json.loads((target/'native_sql_tools.json').read_text())
                        if t['displayName']=='get_order_risk')
            self.assertIn('order_risk_response',lookup['toolConfig']['query'])

    def test_exact_saved_contract_and_registered_run_required(self):
        proof=evidence()
        self.assertEqual(proof.contract,feature_contract())
        self.assertEqual(len(proof.sha256),64)
        contract=proof.contract;contract['numeric'].append('late_probability')
        with self.assertRaises(ValueError):TrainingFeatureEvidence(json.dumps(contract).encode(),MODEL,'1',RUN)
        for model,version,run in [(MODEL,'2',RUN),(MODEL,'1','wrong'),('wrong','1',RUN)]:
            with self.subTest(version=version,run=run),self.assertRaises(ValueError):
                TrainingFeatureEvidence(proof.artifact_bytes,model,version,run)
        with self.assertRaises(ValueError):TrainingFeatureEvidence.from_native_records(
            proof.artifact_bytes,{'name':MODEL,'version':'1','run_id':RUN},{'run_id':RUN,'status':'RUNNING'})

    def test_only_order_lookup_changes_routing_and_other_tools_preserved(self):
        tools=native_tools(fixtures.receipt(),environment());before=deepcopy(tools)
        updated=apply_order_risk_contract(tools,fixtures.SNAPSHOT,evidence())
        self.assertEqual(tools,before)
        for old,new in zip(tools,updated):
            if old['displayName']!='get_order_risk':self.assertEqual(old,new)
            else:
                conf=deepcopy(new['toolConfig']);conf['query']=old['toolConfig']['query']
                self.assertEqual(conf,old['toolConfig'])
                self.assertEqual(new['inputSchema'],old['inputSchema'])
                self.assertEqual(new['key'],old['key'])
        self.assertEqual(updated,apply_order_risk_contract(updated,fixtures.SNAPSHOT,evidence()))
        with self.assertRaises(ValueError):apply_order_risk_contract([],fixtures.SNAPSHOT,evidence())

    def test_nested_response_values_roles_dates_and_no_individual_attribution(self):
        fixture=fixtures.QueryTests();fixture.setUp();db=fixture.db
        try:
            for name,value in [('transit_distance_km',123.5),('line_count',3),('units',8)]:
                db.execute('ALTER TABLE scoring ADD COLUMN '+name)
                db.execute('UPDATE scoring SET '+name+'=?',(value,))
            def named(*args):
                values={}
                for key,val in zip(args[::2],args[1::2]):
                    if isinstance(val,str) and val.startswith(('{','[')):
                        try:val=json.loads(val)
                        except ValueError:pass
                    values[key]=val
                return json.dumps(values)
            db.create_function('named_struct',-1,named)
            db.create_function('array',-1,lambda *args:json.dumps(args))
            db.create_function('to_json',1,lambda value:value)
            db.create_function('unix_micros',1,lambda value:int(datetime.fromisoformat(value).timestamp()*1_000_000))
            db.create_function('regexp',2,lambda pattern,value:__import__('re').search(pattern,value) is not None)
            def reject(reason):raise ValueError(reason)
            db.create_function('raise_error',1,reject)
            sql=order_risk_response_query(fixtures.SNAPSHOT,evidence())
            sql=sql.replace(PREDICTIONS+' VERSION AS OF 2','predictions').replace(SCORING+' VERSION AS OF 1','scoring').replace(' RLIKE ',' REGEXP ')
            def lookup(key):return [json.loads(row[0]) for row in db.execute(sql.replace('{{order_id}}',key))]
            result=lookup('O-00030001')[0]
            self.assertEqual(set(result),{'schema_version','prediction','model_inputs','derivation_source_dates','metadata','attribution'})
            self.assertEqual(result['prediction']['late_probability'],.4)
            inputs=result['model_inputs']['feature_values']
            self.assertEqual(set(inputs),set(feature_contract()['categorical']+feature_contract()['numeric']))
            self.assertTrue(set(PREDICTION_FIELDS).isdisjoint(inputs))
            self.assertEqual(inputs['promised_lead_days'],3)
            self.assertEqual((inputs['transit_distance_km'],inputs['line_count'],inputs['units']),(123.5,3,8))
            self.assertNotIn('accepted_at',inputs);self.assertNotIn('order_id',inputs)
            self.assertIn('accepted_at',result['derivation_source_dates'])
            self.assertEqual(result['metadata']['feature_contract_sha256'],evidence().sha256)
            self.assertFalse(result['attribution']['individual_feature_attribution_available'])
            self.assertFalse(result['attribution']['causal_explanation_available'])
            # SQLite has integer booleans; normalize only this local fixture to
            # the native JSON boolean contract before exercising the validator.
            native_shape=deepcopy(result)
            native_shape['prediction']['predicted_late']=bool(native_shape['prediction']['predicted_late'])
            native_shape['derivation_source_dates']['separate_fitted_features']=False
            native_shape['attribution']['individual_feature_attribution_available']=False
            native_shape['attribution']['causal_explanation_available']=False
            self.assertEqual(validate_order_risk_response(native_shape,fixtures.SNAPSHOT,evidence())['status'],
                             'ORDER RISK CONTRACT VERIFIED')
            for mutate in [lambda x:x['model_inputs']['feature_values'].update(late_probability=.4),
                           lambda x:x['model_inputs']['feature_values'].update(promised_lead_days=3.1),
                           lambda x:x['metadata'].update(feature_contract_sha256='wrong'),
                           lambda x:x['attribution'].update(individual_feature_attribution_available=True)]:
                bad=deepcopy(native_shape);mutate(bad)
                with self.assertRaises(ValueError):validate_order_risk_response(bad,fixtures.SNAPSHOT,evidence())
            self.assertEqual(lookup('O-99999999'),[])
            with self.assertRaises(sqlite3.OperationalError):lookup('O-1')
        finally:fixture.tearDown()
