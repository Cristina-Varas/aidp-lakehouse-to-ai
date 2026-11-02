"""Observation classifiers with explicit fixtures, not cloud evidence."""
from copy import deepcopy
import unittest
from aidp_lakehouse.agent_native_verification import (parameter_cases,classify_native_result,
    semantic_graph_sha256)


class DirectNativeEvidenceTests(unittest.TestCase):
    def test_raw_json_types_include_invalid_values_without_normalization(self):
        cases=parameter_cases('O-00030255','C-000550');self.assertEqual(len(cases),21)
        by_id={c['id']:c for c in cases}
        self.assertIs(by_id['rank_boolean']['values']['limit'],True)
        self.assertEqual(by_id['rank_numeric_string']['values']['limit'],'10')
        self.assertEqual(by_id['rank_fractional']['values']['limit'],10.5)
        self.assertEqual(by_id['order_invalid_format']['values']['order_id'],'O-123')
        with self.assertRaises(ValueError):parameter_cases('bad','C-000550')

    def test_timeout_empty_result_and_generic_server_error_are_not_rejections(self):
        case=parameter_cases('O-00030255','C-000550')[2]
        self.assertEqual(classify_native_result(case,{'timed_out':True,'timeout_seconds':150})['outcome'],'UNKNOWN')
        empty={'status':200,'body':{'result':{'rows':[]}}}
        self.assertEqual(classify_native_result(case,empty)['observation'],'EMPTY_RESULT')
        self.assertEqual(classify_native_result(case,{'status':500,'body':{'code':'InternalError'}})['outcome'],'UNKNOWN')
        self.assertEqual(classify_native_result(case,{'status':400,'body':{'code':'InvalidParameter'}})['observation'],'REJECTED_REQUEST')
        self.assertEqual(classify_native_result(case,{'status':401,'body':{}})['outcome'],'NOT_A_PARAMETER_VERDICT')

    def test_extra_argument_and_numeric_string_classification_require_observed_baseline(self):
        cases={c['id']:c for c in parameter_cases('O-00030255','C-000550')}
        success={'status':200,'body':{'result':{'rows':[{'value':'fixture'}]}}}
        self.assertEqual(classify_native_result(cases['order_extra'],success,success)['observation'],'EXTRA_ARGUMENT_NO_RESULT_EFFECT')
        self.assertEqual(classify_native_result(cases['rank_numeric_string'],success,success)['observation'],'NUMERIC_STRING_ACCEPTED_WITH_CONFIGURED_SQL_CAST')
        self.assertEqual(classify_native_result(cases['rank_numeric_string'],success)['observation'],'RETURNED_ROWS')
        wrapped={'status':200,'body':{'result':{'success':False,'error':'fixture failure','rows':[]}}}
        self.assertEqual(classify_native_result(cases['order_extra'],wrapped)['outcome'],'UNKNOWN')

    def test_final_configuration_hash_ignores_audit_but_detects_any_operational_change(self):
        graph={'llm':{'modelId':'selected'},'tools':[{'config':{'query':'fixed'},'schema':{},'timeUpdated':'one'}]}
        audit=deepcopy(graph);audit['tools'][0]['timeUpdated']='two'
        self.assertEqual(semantic_graph_sha256(graph),semantic_graph_sha256(audit))
        for mutate in [lambda x:x['llm'].update(modelId='changed'),
                       lambda x:x['tools'][0]['config'].update(query='changed'),
                       lambda x:x['tools'][0]['schema'].update(limit='changed')]:
            changed=deepcopy(graph);mutate(changed)
            self.assertNotEqual(semantic_graph_sha256(graph),semantic_graph_sha256(changed))
