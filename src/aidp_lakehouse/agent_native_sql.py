"""Prepare native SQL tools without a Custom Code package or cloud mutations.

The static profile retains the pinned queries verbatim. The complete profile
adds guarded parameter templates; cloud verification remains a separate gate.
Environment-specific exports and generated definitions belong outside Git.
"""
import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import uuid

from .agent_bundle import validated_snapshot
from .agent_queries import TOOLS, PREDICTIONS, SCORING, native_sql_config, query_sql

STATIC_TOOLS = ('risk_summary', 'risk_by_carrier', 'risk_by_region')
CANVAS_ROUTING = ('queryType', 'catalogKey', 'schemaKey', 'sparkComputeKey')


def native_parameter_query(name, snapshot):
    """Fixed SQL templates with SQL-side guards, never caller-supplied SQL.

    Oracle's native string placeholders are quoted. The native binder must
    handle escaping; the guard enforces the business format before matching.
    Invalid inputs raise, rather than appearing to be an absent business key.
    This function prepares SQL; it does not assert native runtime verification.
    """
    if name in ('get_order_risk', 'get_customer_orders'):
        field, prefix, digits = (('order_id', 'O', 8) if name == 'get_order_risk'
                                 else ('customer_id', 'C', 6))
        sentinel = prefix + '-' + '0' * digits
        sql = query_sql(name, {field: sentinel}, snapshot)
        value = "'{{" + field + "}}'"
        pattern = '^' + prefix + '-' + '[0-9]' * digits + '$'
        guard = (f"CASE WHEN LENGTH({value}) = {digits + 2} AND {value} RLIKE '{pattern}' THEN {value} "
                 f"ELSE CAST(raise_error('INVALID_{field.upper()}') AS STRING) END")
        literal = "'" + sentinel + "'"
        if sql.count(literal) != 1:
            raise ValueError('Unexpected source query; refuse template substitution')
        return sql.replace(literal, '(' + guard + ')')
    if name != 'rank_open_orders':
        raise ValueError('Only the three explicit parameterized queries are supported')
    # Rank at most 20 rows with the original deterministic ordering, then apply
    # the validated requested size. A fixed LIMIT also bounds native results.
    base = query_sql(name, {'limit': 20}, snapshot)
    value = "'{{limit}}'"
    number = f'TRY_CAST({value} AS DECIMAL(38, 20))'
    guard = (f"CASE WHEN {value} RLIKE '^[0-9]+([.][0-9]+)?$' AND NOT ({value} RLIKE '[^0-9.]') "
             f'AND {number} BETWEEN 1 AND 20 AND {number} = FLOOR({number}) '
             f"THEN CAST({number} AS INT) ELSE CAST(raise_error('INVALID_LIMIT') AS INT) END")
    fields = ('risk_rank, order_id, customer_id, late_probability, predicted_late, '
              'decision_threshold, model_name, model_version, training_run_id, '
              'training_cutoff, input_delta_version, accepted_at, '
              'promised_delivery_at_acceptance, priority_at_acceptance, '
              'planned_carrier_at_acceptance, customer_region_at_acceptance, '
              'customer_segment_at_acceptance, inventory_fill_rate_at_acceptance, '
              'warehouse_load_at_acceptance, _reference_date')
    return (f'WITH bounded_ranking AS (\n{base}\n)\nSELECT {fields}\n'
            f'FROM bounded_ranking WHERE risk_rank <= ({guard})\n'
            'ORDER BY late_probability DESC, order_id ASC LIMIT 20')


def static_native_tools(receipt, environment):
    """Return native canvas definitions; this does not deploy or verify them."""
    snapshot = validated_snapshot(receipt)
    if receipt.get('observation_date') != '2026-10-28':
        raise ValueError('Require the fixed 2026-10-28 readiness receipt')
    workspace = environment.get('workspace_key')
    connection = environment.get('sql_connection')
    if not isinstance(workspace, str) or not workspace:
        raise ValueError('Require the independently verified project workspace key')
    if not isinstance(connection, dict) or connection.get('queryType') != 'SPARK':
        raise ValueError('Require the successful native Spark SQL configuration export')
    definitions = []
    for name in STATIC_TOOLS:
        checked = native_sql_config(connection, name, {}, snapshot)
        scope = ':'.join((workspace, name, receipt['table_ids'][PREDICTIONS],
                          str(snapshot.predictions_version), receipt['table_ids'][SCORING],
                          str(snapshot.scoring_version)))
        definitions.append({
            'toolType': 'SQL', 'key': str(uuid.uuid5(uuid.NAMESPACE_URL, scope)),
            'workspaceKey': workspace, 'displayName': name,
            'description': TOOLS[name][0], 'inputSchema': {},
            # These are canvas keys from the verified export. The native canvas
            # compiler performs its own translation into the runtime toolkit.
            'toolConfig': {**{key: checked[key] for key in CANVAS_ROUTING},
                           'query': checked['query'], 'isRowLimitEnabled': True,
                           'maxRows': TOOLS[name][2]},
        })
    return definitions


def native_tools(receipt, environment):
    """Prepare all six SQL definitions, including guarded native parameters.

    Runtime verification is a separate gate. No generated file asserts PASS.
    """
    static = {t['displayName']: t for t in static_native_tools(receipt, environment)}
    snapshot = validated_snapshot(receipt)
    definitions = []
    for name, (description, parameters, maximum) in TOOLS.items():
        if name in static:
            definitions.append(static[name])
            continue
        prototype = static['risk_summary']
        schema = {key: {'name': key, 'type': kind,
                       'description': ('Integer from 1 through 20; invalid values must fail.'
                                       if key == 'limit' else 'Exact synthetic ' + key + '; do not repair malformed input.')}
                  for key, kind in parameters.items()}
        if name == 'rank_open_orders':
            schema['limit']['defaultValue'] = 10
        definitions.append({
            'toolType': 'SQL',
            'key': str(uuid.uuid5(uuid.UUID(prototype['key']), name)),
            'workspaceKey': prototype['workspaceKey'], 'displayName': name,
            'description': description, 'inputSchema': schema,
            'toolConfig': {**prototype['toolConfig'],
                           'query': native_parameter_query(name, snapshot), 'maxRows': maximum},
        })
    return definitions


def native_canvas_diagram(existing, sql_tools, rag_tool):
    """Use native tool references and the current editor's structural port names.

    Inline tools execute through the API but are not displayed reliably by the
    canvas. Preserve the selected model, instructions, session configuration and
    all unrelated metadata; stop on a different graph rather than resetting it.
    """
    if set(existing.get('nodes', {})) != {'start', 'planner'}:
        raise ValueError('Unexpected existing graph; preserve it for review')
    if existing['nodes']['planner'].get('type') != 'AGENT':
        raise ValueError('Expected the verified native planner node')
    if ({t.get('displayName') for t in sql_tools} != set(TOOLS)
            or len(sql_tools) != 6 or any(t.get('toolType') != 'SQL' for t in sql_tools)
            or rag_tool.get('toolType') != 'RAG'
            or rag_tool.get('displayName') != 'retrieve_operations_policy'):
        raise ValueError('Require the six native SQL tools and dedicated policy RAG')
    definitions = deepcopy(sql_tools + [rag_tool])
    if len({t.get('key') for t in definitions}) != 7 or not all(t.get('key') for t in definitions):
        raise ValueError('Require distinct stable native tool keys')
    edges = existing.get('edges', {})
    if len(edges) != 1 or any(e.get('srcNodeId') != 'start' or e.get('destNodeId') != 'planner'
                              for e in edges.values()):
        raise ValueError('Unexpected existing flow; preserve its edges')
    diagram = deepcopy(existing)
    for i, tool in enumerate(definitions):
        if tool.get('positionX') is None: tool['positionX'] = 80 + (i % 4) * 260
        if tool.get('positionY') is None: tool['positionY'] = 360 + (i // 4) * 180
    diagram['toolsMap'] = {t['key']: t for t in definitions}
    diagram['nodes']['planner']['tools'] = []
    diagram['nodes']['planner']['toolReferences'] = [t['key'] for t in definitions]
    for edge in diagram['edges'].values():
        edge['srcNodeOutput'], edge['destNodeInput'] = 'START_NODE_OUTPUT', 'AGENT_INPUT'
    return diagram


def write_native_sql_bundle(directory, receipt, environment, profile='static', feature_evidence=None):
    if profile not in ('static', 'complete'):
        raise ValueError('Choose an explicit static or complete native SQL profile')
    tools = (static_native_tools if profile == 'static' else native_tools)(receipt, environment)
    response_contract = None
    if profile == 'complete':
        from .agent_order_risk import apply_order_risk_contract, TrainingFeatureEvidence, SCHEMA
        if not isinstance(feature_evidence, TrainingFeatureEvidence):
            raise ValueError('Complete export requires the saved training feature contract')
        tools = apply_order_risk_contract(tools, validated_snapshot(receipt), feature_evidence)
        response_contract = {'schema': SCHEMA, 'artifact': 'feature_contract.json',
                             'sha256': feature_evidence.sha256,
                             'training_run_id': feature_evidence.training_run_id}
    encoded = (json.dumps(tools, indent=2, sort_keys=True) + '\n').encode()
    manifest = {
        'stage': 'PREPARED_ONLY', 'custom_code_required': False,
        'runtime_verified': False, 'agent_acceptance_pass': False,
        'profile': profile, 'tools': [t['displayName'] for t in tools],
        'order_risk_response_contract': response_contract,
        'excluded_pending_native_parameter_validation': [
            name for name in TOOLS if name not in {t['displayName'] for t in tools}],
        'predictions_version': receipt['predictions_version'],
        'scoring_version': receipt['scoring_version'],
        'table_ids': receipt['table_ids'],
        'files': {'native_sql_tools.json': {'bytes': len(encoded),
                  'sha256': hashlib.sha256(encoded).hexdigest()}},
    }
    planned = {'native_sql_tools.json': encoded,
               'manifest.json': (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode()}
    root = Path(directory)
    if root.exists():
        existing = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}
        if existing != planned:
            raise ValueError('Existing native bundle differs; preserve it and use a new directory')
        return {'status': 'NATIVE SQL BUNDLE PREPARED', 'changed_files': 0, 'tools': len(tools)}
    root.mkdir(parents=True, mode=0o700)
    for name, content in planned.items():
        file = root / name
        file.write_bytes(content)
        file.chmod(0o600)
    return {'status': 'NATIVE SQL BUNDLE PREPARED', 'changed_files': len(planned), 'tools': len(tools)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', required=True, type=Path)
    parser.add_argument('--environment', required=True, type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--profile', choices=('static', 'complete'), default='static')
    parser.add_argument('--feature-contract', type=Path,
                        help='Downloaded feature_contract.json from the exact registered training run')
    args = parser.parse_args()
    receipt = json.loads(args.receipt.read_text())
    evidence = None
    if args.profile == 'complete':
        if args.feature_contract is None:
            parser.error('--profile complete requires --feature-contract')
        from .agent_order_risk import TrainingFeatureEvidence
        evidence = TrainingFeatureEvidence(args.feature_contract.read_bytes(), receipt['model_name'],
                                           str(receipt['model_version']), receipt['training_run_id'])
    print(json.dumps(write_native_sql_bundle(args.output_dir,
          receipt, json.loads(args.environment.read_text()), args.profile, evidence)))


if __name__ == '__main__':
    main()
