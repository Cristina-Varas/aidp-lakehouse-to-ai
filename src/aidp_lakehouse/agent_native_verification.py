"""Direct native Test cases and conservative result classification; no LLM calls.

The Test API receives SQL configuration and raw JSON paramValues. It does not
receive the saved tool inputSchema, so these observations do not certify schema
coercion/defaults of a compiled LLM-facing tool. Never infer rejection from a
client timeout or from an empty business result.
"""
import json
import re


def parameter_cases(order_id, customer_id):
    if not re.fullmatch(r'O-[0-9]{8}',order_id) or not re.fullmatch(r'C-[0-9]{6}',customer_id):
        raise ValueError('Supply independently verified exact business keys')
    return [
        {'id':'order_valid','tool':'get_order_risk','values':{'order_id':order_id}},
        {'id':'order_missing_record','tool':'get_order_risk','values':{'order_id':'O-99999999'}},
        {'id':'order_invalid_format','tool':'get_order_risk','values':{'order_id':'O-123'}},
        {'id':'order_numeric','tool':'get_order_risk','values':{'order_id':123}},
        {'id':'order_null','tool':'get_order_risk','values':{'order_id':None}},
        {'id':'order_omitted','tool':'get_order_risk','values':{}},
        {'id':'order_quote','tool':'get_order_risk','values':{'order_id':order_id+"' OR 1=1 --"}},
        {'id':'order_extra','tool':'get_order_risk','values':{'order_id':order_id,'unexpected':'fixture-only'},'baseline':'order_valid','extra_argument':'unexpected'},
        {'id':'customer_valid','tool':'get_customer_orders','values':{'customer_id':customer_id}},
        {'id':'customer_invalid_format','tool':'get_customer_orders','values':{'customer_id':'C-123'}},
        {'id':'rank_valid','tool':'rank_open_orders','values':{'limit':10}},
        {'id':'rank_numeric_string','tool':'rank_open_orders','values':{'limit':'10'},'baseline':'rank_valid','sql_cast_comparison':True},
        {'id':'rank_fractional','tool':'rank_open_orders','values':{'limit':10.5}},
        {'id':'rank_excess','tool':'rank_open_orders','values':{'limit':21}},
        {'id':'rank_zero','tool':'rank_open_orders','values':{'limit':0}},
        {'id':'rank_boolean','tool':'rank_open_orders','values':{'limit':True}},
        {'id':'rank_null','tool':'rank_open_orders','values':{'limit':None}},
        {'id':'rank_omitted','tool':'rank_open_orders','values':{}},
        {'id':'rank_extra','tool':'rank_open_orders','values':{'limit':10,'unexpected':'fixture-only'},'baseline':'rank_valid','extra_argument':'unexpected'},
        {'id':'summary_valid','tool':'risk_summary','values':{}},
        {'id':'summary_extra','tool':'risk_summary','values':{'unexpected':'fixture-only'},'baseline':'summary_valid','extra_argument':'unexpected'},
    ]


def json_type(value):
    if value is None:return 'null'
    if isinstance(value,bool):return 'boolean'
    if isinstance(value,(int,float)):return 'number'
    if isinstance(value,str):return 'string'
    if isinstance(value,list):return 'array'
    if isinstance(value,dict):return 'object'
    raise ValueError('Unsupported JSON parameter type')


def visible_rows(response):
    body=response.get('body') or {}
    if isinstance(body,dict) and (body.get('success') is False or body.get('error')):return None
    result=body.get('result') if isinstance(body,dict) else None
    if response.get('status')!=200 or not isinstance(result,dict):return None
    if result.get('error') or result.get('success') is False:return None
    rows=result.get('rows')
    return rows if isinstance(rows,list) else None


def classify_native_result(case, response, baseline=None):
    record={'case':case['id'],'tool':case['tool'],
            'input_json_types':{k:json_type(v) for k,v in case['values'].items()},
            'http_status':response.get('status'), 'native_intermediate_types_observed':False}
    if response.get('timed_out'):
        return {**record,'observation':'TIMEOUT','outcome':'UNKNOWN',
                'timeout_seconds':response.get('timeout_seconds'),
                'reason':'Client deadline reached; no validation rejection inferred.'}
    if response.get('transport_error_class'):
        return {**record,'observation':'TRANSPORT_ERROR','outcome':'UNKNOWN'}
    status=response.get('status');body=response.get('body') or {}
    if status in (401,403):return {**record,'observation':'AUTHORIZATION_ERROR','outcome':'NOT_A_PARAMETER_VERDICT'}
    if isinstance(status,int) and 400<=status<500:
        return {**record,'observation':'REJECTED_REQUEST','outcome':'REJECTED',
                'error_code':body.get('code') if isinstance(body,dict) else None,
                'reason':'Native endpoint rejected the request; validation stage is not inferred from HTTP status alone.'}
    if isinstance(status,int) and status>=500:
        return {**record,'observation':'SERVICE_ERROR','outcome':'UNKNOWN',
                'error_code':body.get('code') if isinstance(body,dict) else None}
    rows=visible_rows(response)
    if rows is None:return {**record,'observation':'NO_VISIBLE_SQL_RESULT','outcome':'UNKNOWN'}
    result={**record,'observation':'RETURNED_ROWS','outcome':'RETURNED','rows':len(rows)}
    if not rows:
        return {**result,'observation':'EMPTY_RESULT','reason':'Zero rows are not proof of parameter rejection.'}
    baseline_rows=visible_rows(baseline) if baseline is not None else None
    if baseline_rows is not None and rows==baseline_rows:
        if case.get('extra_argument'):
            result.update(observation='EXTRA_ARGUMENT_NO_RESULT_EFFECT',
                          ignored_argument=case['extra_argument'],
                          reason='The extra argument was accepted and produced identical visible rows; it was not rejected.')
        elif case.get('sql_cast_comparison'):
            result.update(observation='NUMERIC_STRING_ACCEPTED_WITH_CONFIGURED_SQL_CAST',
                          reason='String input produced the same rows as the numeric baseline. SQL contains explicit TRY_CAST; native intermediate coercion is not visible.')
    return result


def semantic_graph_sha256(diagram):
    """Fingerprint every operational field; omit only service audit timestamps."""
    import hashlib
    audit={'timeCreated','timeUpdated','createdBy','updatedBy','validationErrors'}
    def canonical(value):
        if isinstance(value,dict):return {k:canonical(v) for k,v in value.items() if k not in audit}
        if isinstance(value,list):return [canonical(v) for v in value]
        return value
    return hashlib.sha256(json.dumps(canonical(diagram),sort_keys=True,separators=(',',':')).encode()).hexdigest()
