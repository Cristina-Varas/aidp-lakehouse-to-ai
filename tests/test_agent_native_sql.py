"""Local definition/reproducibility checks; not native AIDP execution proof."""
from copy import deepcopy
import json
import math
from pathlib import Path
import re
import sqlite3
import tempfile
import unittest

from aidp_lakehouse.agent_native_sql import (STATIC_TOOLS, static_native_tools,
    write_native_sql_bundle, native_parameter_query, native_tools, native_canvas_diagram)
from aidp_lakehouse.agent_queries import AgentSnapshot, query_sql, PREDICTIONS, SCORING, TOOLS
from test_operations_agent import receipt
import test_operations_agent as fixtures


def environment():
    return {'workspace_key': 'test-project-workspace', 'sql_connection': {
        'queryType': 'SPARK', 'catalogKey': 'lakehouse_demo',
        'schemaKey': 'gold', 'sparkComputeKey': 'test-project-compute'}}


class NativeSqlDefinitionTests(unittest.TestCase):
    def test_canvas_roundtrip_preserves_model_instructions_and_native_definitions(self):
        tools = native_tools(receipt(), environment())
        rag = {'key': 'rag-key', 'toolType': 'RAG', 'displayName': 'retrieve_operations_policy',
               'toolConfig': {'knowledgeBase': 'operations_policies'}}
        base = {'nodes': {'start': {'type': 'START_NODE'}, 'planner': {
            'type': 'AGENT', 'llm': {'modelId': 'owner-model'}, 'instructions': 'Owner instructions',
            'tools': tools + [rag]}}, 'edges': {'edge': {'srcNodeId': 'start',
            'destNodeId': 'planner', 'srcNodeOutput': 'output', 'destNodeInput': 'input'}},
            'guardrailsMap': {'owner-guard': {}}}
        original = deepcopy(base)
        diagram = native_canvas_diagram(base, tools, rag)
        self.assertEqual(base, original)
        self.assertEqual(diagram['edges']['edge']['srcNodeOutput'], 'START_NODE_OUTPUT')
        self.assertEqual(diagram['edges']['edge']['destNodeInput'], 'AGENT_INPUT')
        legacy = deepcopy(base)
        legacy['edges']['edge'].update(srcNodeOutput='bottom', destNodeInput='top')
        self.assertEqual(native_canvas_diagram(legacy, tools, rag), diagram)
        self.assertEqual(diagram['nodes']['planner']['llm'], base['nodes']['planner']['llm'])
        self.assertEqual(diagram['nodes']['planner']['instructions'], 'Owner instructions')
        self.assertEqual(diagram['guardrailsMap'], base['guardrailsMap'])
        self.assertEqual(diagram['nodes']['planner']['tools'], [])
        self.assertEqual(len(diagram['nodes']['planner']['toolReferences']), 7)
        self.assertEqual([t['toolConfig'] for t in diagram['toolsMap'].values()],
                         [t['toolConfig'] for t in tools + [rag]])
        self.assertEqual(diagram, native_canvas_diagram(diagram, list(diagram['toolsMap'].values())[:6],
                                                       list(diagram['toolsMap'].values())[6]))
        bad = deepcopy(base); bad['nodes']['unrelated'] = {}
        with self.assertRaises(ValueError): native_canvas_diagram(bad, tools, rag)
        with self.assertRaises(ValueError): native_canvas_diagram(base, tools[:5], rag)

    def test_full_profile_has_only_six_bounded_native_queries(self):
        tools = native_tools(receipt(), environment())
        self.assertEqual([t['displayName'] for t in tools], list(TOOLS))
        self.assertEqual(len({t['key'] for t in tools}), 6)
        for t in tools:
            self.assertEqual(t['toolType'], 'SQL')
            self.assertEqual(t['toolConfig']['maxRows'], TOOLS[t['displayName']][2])
            self.assertIn('VERSION AS OF 2', t['toolConfig']['query'])
            self.assertIn('VERSION AS OF 1', t['toolConfig']['query'])
            self.assertNotIn('SELECT *', t['toolConfig']['query'])
        self.assertEqual(tools[1]['inputSchema']['limit']['defaultValue'], 10)
        with self.assertRaises(ValueError):
            native_parameter_query('unknown', AgentSnapshot(2, 1))

    def test_static_queries_and_limits_are_preserved_without_custom_code(self):
        proof = receipt()
        tools = static_native_tools(proof, environment())
        self.assertEqual([t['displayName'] for t in tools], list(STATIC_TOOLS))
        self.assertEqual([t['toolConfig']['maxRows'] for t in tools], [1, 3, 4])
        for tool in tools:
            self.assertEqual(tool['toolType'], 'SQL')
            self.assertEqual(tool['inputSchema'], {})
            self.assertEqual(tool['toolConfig']['query'], query_sql(
                tool['displayName'], {}, AgentSnapshot(2, 1)))
            self.assertNotIn('{{', tool['toolConfig']['query'])
            self.assertNotIn('packagePath', tool['toolConfig'])
            self.assertNotIn('catalogType', tool['toolConfig'])

    def test_untrusted_receipt_target_and_credentials_are_rejected(self):
        bad = deepcopy(receipt()); bad['status'] = 'PREPARED_ONLY'
        with self.assertRaises(ValueError): static_native_tools(bad, environment())
        bad = deepcopy(receipt()); bad['observation_date'] = '2026-10-29'
        with self.assertRaises(ValueError): static_native_tools(bad, environment())
        for key, value in [('queryType', 'ORACLE'), ('catalogKey', 'other'),
                           ('schemaKey', 'other'), ('password', 'fixture-only')]:
            env = environment(); env['sql_connection'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                static_native_tools(receipt(), env)

    def test_keys_are_stable_and_scoped_to_workspace_and_snapshot_identity(self):
        proof = receipt(); env = environment()
        original = static_native_tools(proof, env)
        self.assertEqual(original, static_native_tools(proof, env))
        changed_env = deepcopy(env); changed_env['workspace_key'] = 'different-workspace'
        changed_proof = deepcopy(proof); changed_proof['table_ids'][PREDICTIONS] = 'different-table'
        keys = {x['key'] for x in original}
        self.assertTrue(keys.isdisjoint(x['key'] for x in static_native_tools(proof, changed_env)))
        self.assertTrue(keys.isdisjoint(x['key'] for x in static_native_tools(changed_proof, env)))

    def test_identical_rerun_and_conflict_preservation_never_claim_runtime_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'private-native-bundle'
            self.assertEqual(write_native_sql_bundle(path, receipt(), environment())['changed_files'], 2)
            self.assertEqual(write_native_sql_bundle(path, receipt(), environment())['changed_files'], 0)
            manifest = json.loads((path / 'manifest.json').read_text())
            self.assertFalse(manifest['runtime_verified'])
            self.assertFalse(manifest['agent_acceptance_pass'])
            self.assertEqual(len(manifest['excluded_pending_native_parameter_validation']), 3)
            (path / 'manifest.json').write_text('preserve owner edits')
            with self.assertRaises(ValueError): write_native_sql_bundle(path, receipt(), environment())
            self.assertEqual((path / 'manifest.json').read_text(), 'preserve owner edits')


class NativeParameterSemanticsTests(unittest.TestCase):
    """SQLite adaptation of Spark guards; does not test native binding/escaping.

    Real native binding and Spark execution require separately recorded cloud
    probes. These fixtures compare the template's business results with the
    original validated query generator, preserving ties and every output field.
    """
    def setUp(self):
        self.fixture = fixtures.QueryTests()
        self.fixture.setUp()
        self.db = self.fixture.db
        self.db.create_function('regexp', 2, lambda pattern, value: bool(re.search(pattern, value)))
        self.db.create_function('floor', 1, lambda value: None if value is None else math.floor(value))
        def number(value):
            try:
                result = float(value)
                return result if math.isfinite(result) else None
            except (TypeError, ValueError): return None
        self.db.create_function('try_number', 1, number)
        def fail(message): raise ValueError(message)
        self.db.create_function('raise_error', 1, fail)

    def tearDown(self): self.fixture.tearDown()

    def execute_template(self, name, values):
        sql = native_parameter_query(name, AgentSnapshot(2, 1))
        for key, value in values.items():
            # Fixture-only substitution, not a native binder implementation.
            sql = sql.replace('{{' + key + '}}', str(value).replace("'", "''"))
        sql = sql.replace(f'{PREDICTIONS} VERSION AS OF 2', 'predictions')
        sql = sql.replace(f'{SCORING} VERSION AS OF 1', 'scoring')
        sql = sql.replace(' RLIKE ', ' REGEXP ')
        sql = re.sub(r"TRY_CAST\(('(?:[^']|'')*') AS DECIMAL\(38, 20\)\)",
                     r'try_number(\1)', sql)
        return [dict(r) for r in self.db.execute(sql)]

    def test_valid_templates_equal_original_business_queries_including_ties(self):
        for name, values in [('get_order_risk', {'order_id': 'O-00030001'}),
                             ('get_order_risk', {'order_id': 'O-99999999'}),
                             ('get_customer_orders', {'customer_id': 'C-000001'}),
                             ('rank_open_orders', {'limit': 1}),
                             ('rank_open_orders', {'limit': 10.0}),
                             ('rank_open_orders', {'limit': 20})]:
            with self.subTest(name=name, values=values):
                self.assertEqual(self.execute_template(name, values), self.fixture.execute(name, values))

    def test_invalid_identifiers_and_numeric_limits_raise_instead_of_looking_missing(self):
        cases = [('get_order_risk', {'order_id': value}) for value in
                 ['O-123', ' O-00030001', 'O-00030001\r', "O-00030001' OR 1=1 --", 'O-00030001\n']]
        cases += [('get_customer_orders', {'customer_id': 'C-123'})]
        cases += [('rank_open_orders', {'limit': value}) for value in
                  [0, 21, -1, 1.2, 1.00000000001, float('nan'), float('inf'), True, 'bad']]
        for name, values in cases:
            with self.subTest(name=name, values=values), self.assertRaises(sqlite3.OperationalError):
                self.execute_template(name, values)
