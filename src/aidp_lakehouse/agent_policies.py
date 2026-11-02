"""Validate the dedicated policy KB and native retrieval evidence, without I/O.

Native chat may redact tool payloads. The native Test API returns document IDs
and chunks; validate those independently against the two approved source files.
"""
from copy import deepcopy
import re
import uuid

POLICY_LOCATION = '/Volumes/lakehouse_demo/gold/lakehouse_to_ai_policy_sources/documents'
POLICY_FILES = ('incident_escalation.txt', 'order_prioritization.txt')


def validate_policy_kb(metadata, environment):
    """Fail closed on a stale KB identity, binding, chunking or wider source."""
    expected_key = environment.get('policy_kb_key')
    connection = environment.get('sql_connection', {})
    expected = {'key': expected_key, 'displayName': 'operations_policies',
                'catalogKey': 'lakehouse_demo', 'schemaKey': 'gold',
                'workspaceKey': environment.get('workspace_key'),
                'clusterKey': connection.get('sparkComputeKey'),
                'embeddingModelName': 'MULTILINGUAL_E5_SMALL',
                'chunkSize': 1024, 'chunkOverlap': 128, 'lifecycleState': 'ACTIVE'}
    if not all(expected[k] for k in ('key', 'workspaceKey', 'clusterKey')):
        raise ValueError('Require independently verified KB, workspace and compute identities')
    if any(metadata.get(k) != v for k, v in expected.items()):
        raise ValueError('Policy KB identity, binding or configuration differs')
    sources = metadata.get('sources') or []
    if len(sources) != 1:
        raise ValueError('Require exactly the dedicated documents source')
    source = sources[0]
    fields = {'type': 'VOLUME', 'location': POLICY_LOCATION,
              'workspaceKey': expected['workspaceKey'], 'clusterKey': expected['clusterKey'],
              'sourceFilePattern': '*.txt', 'chunkSize': 1024, 'chunkOverlap': 128}
    if any(source.get(k) != v for k, v in fields.items()):
        raise ValueError('Policy source location, binding, filter or chunking differs')
    return {'kb_key': expected_key, 'source_key': source.get('key'),
            'embedding_model': expected['embeddingModelName'],
            'chunk_size': 1024, 'chunk_overlap': 128, 'source_location': POLICY_LOCATION}


def policy_rag_definition(metadata, environment, llm):
    """Native SDK RAG definition; keep the owner's selected model unchanged."""
    proof = validate_policy_kb(metadata, environment)
    if not isinstance(llm, dict) or not llm.get('modelId'):
        raise ValueError('Supply the existing verified native model configuration')
    return {'toolType': 'RAG',
            'key': str(uuid.uuid5(uuid.NAMESPACE_URL, proof['kb_key'] + ':retrieve_operations_policy')),
            'displayName': 'retrieve_operations_policy',
            'description': 'Retrieve fictional order prioritization and incident escalation policies. Cite only actual source filenames and numbered sections.',
            # SDK RagToolInputSchema.query is a string, unlike SQL's flat map.
            'inputSchema': {'query': ''},
            'toolConfig': {'catalogKey': 'lakehouse_demo', 'schemaKey': 'gold',
                           'knowledgeBase': 'operations_policies', 'topK': 4, 'llm': deepcopy(llm)}}


def verify_policy_chunks(result, policy_files, required_sources=POLICY_FILES):
    """Verify visible native Test chunks against exact approved policy text.

    Ingestion collapses whitespace, so compare normalized text only here. The
    independently downloaded source hashes remain byte-exact evidence.
    """
    if set(policy_files) != set(POLICY_FILES) or not set(required_sources) <= set(POLICY_FILES):
        raise ValueError('Only the two approved policy files may be verified')
    if result.get('toolType') != 'RAG':
        raise ValueError('Require a native RAG Test result')
    chunks = result.get('result', {}).get('retrievedChunks') or []
    if not chunks:
        raise ValueError('No visible retrieval evidence')
    documents, ids = set(), set()
    normalized = {name: ' '.join(text.split()) for name, text in policy_files.items()}
    for chunk in chunks:
        identity = chunk.get('documentId', '')
        match = re.fullmatch(re.escape(POLICY_LOCATION) + r'/([^/]+)#chunk_[0-9]+', identity)
        if not match or match[1] not in normalized or identity in ids:
            raise ValueError('Unknown, duplicate or unsupported retrieved source identity')
        content = ' '.join(chunk.get('content', '').split())
        if not content or content not in normalized[match[1]]:
            raise ValueError('Retrieved chunk does not match the approved source')
        documents.add(match[1]); ids.add(identity)
    if not set(required_sources) <= documents:
        raise ValueError('Required policy was not retrieved')
    return {'status': 'POLICY RETRIEVAL VERIFIED', 'documents': sorted(documents),
            'document_ids': sorted(ids), 'chunks': len(chunks)}
