"""Actual sklearn fits and temporal/feature isolation. Optional ML dependencies."""
from importlib.util import find_spec
import unittest

HAS_ML=all(find_spec(n) is not None for n in ('numpy','pandas','sklearn'))
if HAS_ML:
    import numpy as np
    import pandas as pd
    from aidp_lakehouse.gold import CONTRACTS_GOLD,PREDICTORS
    from aidp_lakehouse.training_contract import TrainingConfig,CANDIDATES
    from aidp_lakehouse.training import (model_input,temporal_partitions,train_and_evaluate,
        make_pipeline,binary_metrics,derive_acceptance_features)


def fixture():
    rows=[]
    # Each period contains both classes. Feature/label association is deliberately noisy.
    for group,start in enumerate(('2025-01-01T00:00:00Z','2025-11-01T00:00:00Z','2026-05-01T00:00:00Z')):
        for i in range(30):
            accepted=pd.Timestamp(start)+pd.Timedelta(days=i)
            row=dict(order_id=f'O-{group}-{i:02d}',customer_id=f'C-{i%4}',
                customer_segment_at_acceptance='STANDARD' if i%2 else 'PREMIUM',
                customer_region_at_acceptance='NORTH' if group!=2 else 'WEST',
                priority_at_acceptance='NORMAL' if i%3 else 'HIGH',inventory_fill_rate_at_acceptance=.6+i/100,
                warehouse_load_at_acceptance=.7+i/100,planned_carrier_at_acceptance='CARRIER_A',
                transit_distance_km=100+i*10,line_count=1+i%5,units=10+i,
                promised_delivery_at_acceptance=accepted+pd.Timedelta(days=5),accepted_at=accepted,
                is_late=bool(i%3==0 or i%7==0),outcome_available_at=accepted+pd.Timedelta(days=8),
                _batch_id='initial',_reference_date=pd.Timestamp('2026-10-28').date())
            rows.append(row)
    return pd.DataFrame(rows,columns=[n for n,_ in CONTRACTS_GOLD['late_delivery_training']])


@unittest.skipUnless(HAS_ML,'Requires optional training libraries in isolated ML environment')
class TrainingTests(unittest.TestCase):
    def test_outcomes_at_boundaries_are_strictly_purged_and_test_is_disjoint(self):
        df=fixture();cfg=TrainingConfig()
        df.loc[0,'outcome_available_at']=pd.Timestamp(cfg.validation_start)
        df.loc[30,'outcome_available_at']=pd.Timestamp(cfg.test_start)
        parts,report=temporal_partitions(df,cfg,'2026-10-22T00:00:00Z')
        self.assertNotIn('O-0-00',set(parts['train']['order_id']))
        self.assertIn('O-0-00',set(parts['final_train']['order_id']))
        self.assertNotIn('O-1-00',set(parts['final_train']['order_id']))
        self.assertFalse(set(parts['final_train']['order_id'])&set(parts['test']['order_id']))
        self.assertEqual(report['excluded_unavailable_labels'],{'train':1,'validation':1})
        self.assertEqual(sum(report['cohort_reconciliation'][k] for k in ('train','validation','test','purged')),90)
        self.assertTrue((parts['final_train']['outcome_available_at']<pd.Timestamp(cfg.test_start)).all())
        with self.assertRaises(ValueError):temporal_partitions(df,cfg,'2026-03-01T00:00:00Z')

    def test_explicit_features_lead_derivation_and_train_only_preprocessing(self):
        df=fixture();x=model_input(df)
        derived=derive_acceptance_features(x)
        self.assertTrue((derived['promised_lead_days']==5).all())
        self.assertFalse(set(derived)&{'order_id','customer_id','is_late','outcome_available_at','accepted_at'})
        with self.assertRaises(ValueError):derive_acceptance_features(x.assign(is_late=True))
        model=make_pipeline(CANDIDATES[1],42).fit(x.iloc[:30],df['is_late'].iloc[:30].astype(int))
        before=model.named_steps['preprocessing'].named_transformers_['numeric'].mean_.copy()
        model.predict_proba(x.iloc[60:])
        self.assertTrue(np.array_equal(before,model.named_steps['preprocessing'].named_transformers_['numeric'].mean_))
        encoder=model.named_steps['preprocessing'].named_transformers_['categorical']
        self.assertNotIn('WEST',encoder.categories_[1])
        self.assertAlmostEqual(before[2],float(df['transit_distance_km'].iloc[:30].mean()))

    def test_selected_final_model_reproducible_and_test_labels_never_fit(self):
        df=fixture();a=train_and_evaluate(df,TrainingConfig(),'2026-10-22T00:00:00Z')
        changed=df.copy();changed.loc[60:,'is_late']=~changed.loc[60:,'is_late']
        b=train_and_evaluate(changed,TrainingConfig(),'2026-10-22T00:00:00Z')
        self.assertEqual(a.report['selected_candidate'],b.report['selected_candidate'])
        self.assertEqual(a.report['selected_threshold'],b.report['selected_threshold'])
        self.assertEqual(a.report['validation_candidates'],b.report['validation_candidates'])
        x=model_input(df.iloc[60:])
        np.testing.assert_array_equal(a.model.predict_proba(x),b.model.predict_proba(x))
        c=train_and_evaluate(df.sample(frac=1,random_state=9),TrainingConfig(),'2026-10-22T00:00:00Z')
        self.assertEqual(a.report,c.report)
        self.assertEqual(a.report['future_open_orders_scored'],0)
        self.assertFalse(a.report['test_used_for_training'])
        self.assertEqual(len(a.report['validation_candidates']),5)

    def test_degenerate_metrics_and_invalid_data_are_explicit(self):
        m=binary_metrics([0,0],[.1,.2],.5)
        self.assertIsNone(m['roc_auc']);self.assertIsNone(m['average_precision'])
        self.assertEqual(m['confusion_matrix'],[[2,0],[0,0]])
        df=fixture();df['units']=df['units'].astype(float);df.loc[0,'units']=float('inf')
        with self.assertRaises(ValueError):temporal_partitions(df,TrainingConfig(),'2026-10-22T00:00:00Z')
        df=fixture();df.loc[0,'order_id']=df.loc[1,'order_id']
        with self.assertRaises(ValueError):temporal_partitions(df,TrainingConfig(),'2026-10-22T00:00:00Z')
