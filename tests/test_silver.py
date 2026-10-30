import copy
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import random
import unittest

from aidp_lakehouse.silver import KEYS, CONTRACTS, transform_initial


def fixture():
    return json.loads((Path(__file__).parent/'fixtures/silver_initial.json').read_text())


class SilverTests(unittest.TestCase):
    def check_reconciliation(self, result):
        for entity, s in result.summary.items():
            self.assertEqual(s['raw_rows'],s['duplicate_removals']+s['rejected_rows']+s['accepted_rows'])
            self.assertEqual(s['accepted_rows'],len(result.tables[entity]))
            self.assertEqual(s['rejected_rows'],sum(r['occurrence_count'] for r in result.quarantine if r['entity']==entity))
            keys=[r[KEYS[entity]] for r in result.tables[entity]]
            self.assertEqual(len(keys),len(set(keys)))

    def test_clean_initial_types_payload_utc_and_repeatability(self):
        raw=fixture();before=copy.deepcopy(raw)
        first=transform_initial(raw);second=transform_initial(raw)
        self.assertEqual(raw,before)
        self.assertFalse(first.quarantine)
        self.assertEqual(first.business_fingerprints,second.business_fingerprints)
        self.assertEqual(first.summary,second.summary)
        self.assertEqual(first.tables,second.tables)
        for entity,rows in first.tables.items():
            self.assertEqual(len(rows),len(raw[entity]))
            for row in rows:
                for name,dtype in CONTRACTS[entity]:
                    if dtype=='timestamp':self.assertEqual(row[name].tzinfo,timezone.utc)
        order=first.tables['orders'][0]
        self.assertIsInstance(order['units'],int)
        self.assertIsInstance(order['inventory_fill_rate_at_acceptance'],Decimal)
        incident=next(e for e in first.tables['events'] if e['event_type']=='INCIDENT_OPENED')
        self.assertEqual(incident['payload'],{'exception_type':'TRANSPORT'})
        self.check_reconciliation(first)

    def test_invalid_date_and_numeric_have_multiple_reasons_one_rejection(self):
        raw=fixture();bad=raw['orders'][0];key=bad['order_id']
        bad['accepted_at']='2026-02-30T00:00:00Z';bad['units']='not-a-number'
        bad['warehouse_load_at_acceptance']='NaN'
        result=transform_initial(raw)
        q=[r for r in result.quarantine if r['entity']=='orders' and r['business_key']==key]
        self.assertEqual(len(q),1);self.assertEqual(q[0]['occurrence_count'],1)
        self.assertTrue({'INVALID_CONVERSION:accepted_at','INVALID_CONVERSION:units',
                         'INVALID_CONVERSION:warehouse_load_at_acceptance'}<=set(q[0]['rejection_reasons']))
        self.assertEqual(json.loads(q[0]['raw_record'])['accepted_at'],bad['accepted_at'])
        self.assertEqual(q[0]['batch_id'],'initial');self.assertEqual(q[0]['source_file'],'fixtures/orders.csv')
        self.assertEqual(result.summary['orders']['rejected_rows'],1)
        self.check_reconciliation(result)

    def test_unknown_reference_cascades_without_orphans(self):
        raw=fixture();raw['orders'][1]['customer_id']='C-NOT-PRESENT'
        result=transform_initial(raw)
        self.assertEqual(result.summary['orders']['rejected_rows'],1)
        self.assertEqual(result.summary['events']['rejected_rows'],1)
        customers={r['customer_id'] for r in result.tables['customers']}
        orders={r['order_id'] for r in result.tables['orders']}
        self.assertTrue(all(r['customer_id'] in customers for r in result.tables['orders']))
        self.assertTrue(all(r['order_id'] in orders for r in result.tables['deliveries']+result.tables['events']))
        self.check_reconciliation(result)

    def test_exact_duplicates_removed_and_input_order_does_not_select_winners(self):
        raw=fixture();expected=transform_initial(raw)
        for rows in raw.values():rows.extend([copy.deepcopy(rows[0]),copy.deepcopy(rows[0])])
        result=transform_initial(raw)
        self.assertEqual(result.business_fingerprints,expected.business_fingerprints)
        for s in result.summary.values():self.assertEqual(s['duplicate_removals'],2)
        shuffled=copy.deepcopy(raw)
        for rows in shuffled.values():random.Random(11).shuffle(rows)
        again=transform_initial(shuffled)
        self.assertEqual(result.tables,again.tables);self.assertEqual(result.summary,again.summary)
        self.check_reconciliation(result)

    def test_version_then_timestamp_ordering_is_deterministic(self):
        raw=fixture();old=raw['customers'][0]
        v2={**old,'row_version':'2','source_updated_at':'2026-10-25T00:00:00Z','customer_name':'Version two'}
        latest={**v2,'source_updated_at':'2026-10-26T00:00:00Z','customer_name':'Latest snapshot'}
        raw['customers'].extend([latest,v2])
        a=transform_initial(raw)
        self.assertEqual(a.tables['customers'][0]['customer_name'],'Latest snapshot')
        self.assertEqual(a.summary['customers']['duplicate_removals'],2)
        raw['customers'].reverse();b=transform_initial(raw)
        self.assertEqual(a.tables,b.tables)
        self.check_reconciliation(a)

    def test_equal_precedence_conflicts_quarantine_all_copies_not_arbitrary_row(self):
        raw=fixture();raw['customers'].append({**raw['customers'][0],'customer_name':'Conflicting name'})
        a=transform_initial(raw)
        self.assertEqual(a.summary['customers']['rejected_rows'],2)
        q=[r for r in a.quarantine if r['entity']=='customers']
        self.assertTrue(all('CONFLICTING_EQUAL_PRECEDENCE' in r['rejection_reasons'] for r in q))
        raw['customers'].reverse();b=transform_initial(raw)
        self.assertEqual(a.tables,b.tables);self.assertEqual(a.quarantine,b.quarantine)
        self.check_reconciliation(a)

    def test_immutable_acceptance_values_cannot_change_in_a_new_version(self):
        raw=fixture();old=raw['orders'][0]
        changed={**old,'row_version':'2','source_updated_at':'2026-10-27T00:00:00Z',
                 'priority_at_acceptance':'URGENT' if old['priority_at_acceptance']!='URGENT' else 'NORMAL'}
        raw['orders'].append(changed)
        result=transform_initial(raw)
        self.assertEqual(result.summary['orders']['rejected_rows'],2)
        self.assertTrue(all('IMMUTABLE_SOURCE_ATTRIBUTES_CHANGED' in r['rejection_reasons']
                            for r in result.quarantine if r['entity']=='orders'))
        self.check_reconciliation(result)

    def test_duplicate_invalid_occurrences_count_once_per_source_record_not_per_reason(self):
        raw=fixture();bad=raw['customers'][0];bad['customer_name']='';bad['row_version']='oops'
        raw['customers'].append(copy.deepcopy(bad))
        result=transform_initial(raw)
        q=[r for r in result.quarantine if r['entity']=='customers']
        self.assertEqual(len(q),1);self.assertEqual(q[0]['occurrence_count'],2)
        self.assertGreaterEqual(len(q[0]['rejection_reasons']),2)
        self.check_reconciliation(result)

    def test_documented_ranges_and_chronology_are_validated(self):
        for field,value,reason in [('units','4','OUT_OF_RANGE:units'),('line_count','13','OUT_OF_RANGE:line_count'),
                                   ('inventory_fill_rate_at_acceptance','.449','OUT_OF_RANGE:inventory_fill_rate_at_acceptance'),
                                   ('promised_delivery_at_acceptance','2020-01-01T00:00:00Z','PROMISE_NOT_AFTER_ACCEPTANCE')]:
            with self.subTest(field=field):
                raw=fixture();raw['orders'][0][field]=value
                result=transform_initial(raw)
                self.assertTrue(any(reason in r['rejection_reasons'] for r in result.quarantine if r['entity']=='orders'))
                self.check_reconciliation(result)

    def test_no_incremental_batch_is_accepted(self):
        with self.assertRaises(ValueError):transform_initial(fixture(),batch_id='incremental')

    def test_timezone_offsets_normalize_to_the_same_utc_business_values(self):
        from datetime import timedelta
        from aidp_lakehouse.silver import utc_timestamp
        raw=fixture();expected=transform_initial(raw)
        original=raw['customers'][0]['source_created_at']
        raw['customers'][0]['source_created_at']=utc_timestamp(original).astimezone(timezone(timedelta(hours=2))).isoformat()
        self.assertEqual(transform_initial(raw).business_fingerprints,expected.business_fingerprints)

    def test_thin_silver_notebook_structure_and_unexecuted_status(self):
        import ast
        try:import nbformat
        except ImportError:self.skipTest('Install .[notebooks] for notebook-format validation')
        path=Path(__file__).resolve().parents[1]/'notebooks/02_build_silver.ipynb'
        notebook=nbformat.read(path,as_version=4);nbformat.validate(notebook)
        self.assertEqual(notebook.metadata.aidp.runtime_verification,'pending')
        for cell in notebook.cells:
            if cell.cell_type=='code':
                ast.parse(cell.source)
                self.assertIsNone(cell.execution_count);self.assertEqual(cell.outputs,[])


if __name__=='__main__':unittest.main()
