"""Build a deterministic SQL+RAG handoff bundle from a read-only data receipt.

Does not create an agent, upload files, call an LLM or ingest a knowledge base.
"""
import argparse
import hashlib
from importlib.resources import files
import io
import json
from pathlib import Path
import zipfile
from .agent_queries import AgentSnapshot, TOOLS, MODEL, RUN, PREDICTIONS, SCORING

CLASSES = dict(zip(TOOLS, ('RiskSummary', 'RankOpenOrders', 'GetOrderRisk',
                         'GetCustomerOrders', 'RiskByCarrier', 'RiskByRegion')))


def _json(value):
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + '\n').encode()


def bundle_files(receipt):
    """Only a data readiness receipt unlocks generation, never agent acceptance."""
    snapshot = AgentSnapshot(receipt['predictions_version'], receipt['scoring_version'])
    if (receipt.get('status') != 'AGENT DATA READY' or receipt.get('rows') != 600
            or receipt.get('model_name') != MODEL or receipt.get('model_version') != '1'
            or receipt.get('training_run_id') != RUN
            or receipt.get('alerts', -1) + receipt.get('non_alerts', -1) != 600
            or set(receipt.get('table_ids', {})) != {PREDICTIONS, SCORING}
            or not all(isinstance(v, str) and v for v in receipt['table_ids'].values())):
        raise ValueError('Require a complete, matching AGENT DATA READY receipt')
    resource = files('aidp_lakehouse')
    defaults = {'predictions_version': snapshot.predictions_version, 'scoring_version': snapshot.scoring_version,
                'prediction_table_id': receipt['table_ids'][PREDICTIONS], 'scoring_table_id': receipt['table_ids'][SCORING],
                'sql_connection': {}}
    tools = []
    for name, (description, params, _) in TOOLS.items():
        tools.append({'toolClassName': CLASSES[name], 'displayName': name, 'description': description,
                      'version': '0.6.0', 'schema': [{'name': key, 'type': kind,
                           'description': 'Exact ' + key + (' (integer 1 through 20)' if key == 'limit' else '')}
                          for key, kind in params.items()], 'conf': defaults})
    zip_files = {
        'tool_implementation.py': resource.joinpath('agent_resources/tool_implementation.py.txt').read_bytes(),
        'agent_queries.py': resource.joinpath('agent_queries.py').read_bytes(),
        'tool_config.json': _json({'displayName': 'Quistor read-only operations SQL',
                                 'description': 'Pinned synthetic data; native Spark SQL routing required.', 'tools': tools}),
    }
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in sorted(zip_files.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, content)
    result = {'operations_sql_tools.zip': stream.getvalue(),
              'agent_instructions.txt': resource.joinpath('agent_resources/instructions.txt').read_bytes(),
              'data_readiness.json': _json(receipt)}
    for name in ('order_prioritization', 'incident_escalation'):
        # Preserve original bytes; only change the ingestion file extension.
        result['documents/' + name + '.txt'] = resource.joinpath('policies/' + name + '.md').read_bytes()
    result['manifest.json'] = _json({'stage': 'PREPARED_ONLY', 'agent_deployed': False,
        'native_sql_tool_verified': False, 'rag_verified': False,
        'files': {name: {'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)}
                  for name, content in sorted(result.items())},
        'policy_sources': {name + '.txt': 'src/aidp_lakehouse/policies/' + name + '.md'
                           for name in ('order_prioritization', 'incident_escalation')}})
    return result


def write_agent_bundle(directory, receipt):
    planned = bundle_files(receipt)
    root = Path(directory)
    # Validate all existing paths before the first write. Repeated identical
    # builds are safe; changed or unrelated content is preserved and rejected.
    if root.exists():
        existing = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}
        if existing != planned:
            raise ValueError('Existing bundle differs; use a separate empty output directory')
        return {'status': 'BUNDLE PREPARED', 'files': len(planned), 'changed_files': 0}
    root.mkdir(parents=True, exist_ok=False)
    for name, content in planned.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return {'status': 'BUNDLE PREPARED', 'files': len(planned), 'changed_files': len(planned)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', required=True, type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    receipt = json.loads(args.receipt.read_text())
    print(json.dumps(write_agent_bundle(args.output_dir, receipt), indent=2))


if __name__ == '__main__':
    main()
