"""Pure metadata and native Test evidence fixtures, not cloud verification."""
from copy import deepcopy
from importlib.resources import files
import unittest

from aidp_lakehouse.agent_policies import (POLICY_FILES, POLICY_LOCATION,
    validate_policy_kb, policy_rag_definition, verify_policy_chunks)


def metadata():
    return {'key': 'new-kb-id', 'displayName': 'operations_policies',
            'catalogKey': 'lakehouse_demo', 'schemaKey': 'gold',
            'workspaceKey': 'owned-workspace', 'clusterKey': 'owned-compute',
            'embeddingModelName': 'MULTILINGUAL_E5_SMALL', 'chunkSize': 1024,
            'chunkOverlap': 128, 'lifecycleState': 'ACTIVE',
            'sources': [{'key': 'source-id', 'type': 'VOLUME', 'location': POLICY_LOCATION,
                         'workspaceKey': 'owned-workspace', 'clusterKey': 'owned-compute',
                         'sourceFilePattern': '*.txt', 'chunkSize': 1024, 'chunkOverlap': 128}]}


def environment():
    return {'policy_kb_key': 'new-kb-id', 'workspace_key': 'owned-workspace',
            'sql_connection': {'sparkComputeKey': 'owned-compute'}}


def sources():
    return {name: files('aidp_lakehouse').joinpath('policies/' + name.replace('.txt', '.md')).read_text()
            for name in POLICY_FILES}


def result():
    return {'toolType': 'RAG', 'result': {'answer': 'Fixture answer', 'retrievedChunks': [
        {'documentId': POLICY_LOCATION + '/' + name + '#chunk_0',
         'content': ' '.join(text.split()), 'score': .9} for name, text in sources().items()]}}


class PolicyEvidenceTests(unittest.TestCase):
    def test_new_identity_binding_and_only_documents_are_required(self):
        proof = validate_policy_kb(metadata(), environment())
        self.assertEqual(proof['kb_key'], 'new-kb-id')
        for key, value in [('key', 'old-id'), ('workspaceKey', 'other'), ('clusterKey', 'other'),
                           ('chunkSize', 500), ('chunkOverlap', 100),
                           ('embeddingModelName', 'unknown'), ('lifecycleState', 'FAILED')]:
            bad = metadata(); bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): validate_policy_kb(bad, environment())
        for field, value in [('location', POLICY_LOCATION.rsplit('/', 1)[0]),
                             ('sourceFilePattern', '*.txt,*.json'), ('workspaceKey', 'other')]:
            bad = metadata(); bad['sources'][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): validate_policy_kb(bad, environment())
        for extra in [[], metadata()['sources'] * 2]:
            bad = metadata(); bad['sources'] = extra
            with self.assertRaises(ValueError): validate_policy_kb(bad, environment())

    def test_native_sdk_schema_and_selected_model_preserved_without_mutation(self):
        llm = {'modelId': 'owner-selected-model', 'provider': 'owner-provider'}
        tool = policy_rag_definition(metadata(), environment(), llm)
        self.assertEqual(tool['inputSchema'], {'query': ''})
        self.assertEqual(tool['toolConfig']['knowledgeBase'], 'operations_policies')
        self.assertEqual(tool['toolConfig']['llm'], llm)
        tool['toolConfig']['llm']['modelId'] = 'changed'
        self.assertEqual(llm['modelId'], 'owner-selected-model')
        changed = metadata(); changed['key'] = 'another-new-id'
        env = environment(); env['policy_kb_key'] = changed['key']
        self.assertNotEqual(policy_rag_definition(metadata(), environment(), llm)['key'],
                            policy_rag_definition(changed, env, llm)['key'])

    def test_real_chunk_shapes_match_sources_and_reject_fabricated_citations_or_text(self):
        self.assertEqual(verify_policy_chunks(result(), sources())['documents'], list(POLICY_FILES))
        bad = result(); bad['result']['retrievedChunks'][0]['documentId'] = '/other/doc.txt#chunk_0'
        with self.assertRaises(ValueError): verify_policy_chunks(bad, sources())
        bad = result(); bad['result']['retrievedChunks'][0]['content'] = 'Invented policy'
        with self.assertRaises(ValueError): verify_policy_chunks(bad, sources())
        bad = result(); bad['result']['retrievedChunks'].pop()
        with self.assertRaises(ValueError): verify_policy_chunks(bad, sources())
        bad = result(); bad['result']['retrievedChunks'] *= 2
        with self.assertRaises(ValueError): verify_policy_chunks(bad, sources())
        with self.assertRaises(ValueError): verify_policy_chunks({'toolType': 'RAG', 'result': {}}, sources())
