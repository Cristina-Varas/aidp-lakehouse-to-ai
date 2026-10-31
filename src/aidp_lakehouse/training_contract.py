"""Initial temporal training configuration and feature roles; no ML dependencies."""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from .gold import GoldConfig, PREDICTORS

VERSION = '0.4.0'
CATEGORICAL = ('customer_segment_at_acceptance','customer_region_at_acceptance',
               'priority_at_acceptance','planned_carrier_at_acceptance')
ORIGINAL_NUMERIC = ('inventory_fill_rate_at_acceptance','warehouse_load_at_acceptance',
                    'transit_distance_km','line_count','units')
NUMERIC = (*ORIGINAL_NUMERIC,'promised_lead_days')
# Finite search fixed before any validation/test inspection.
CANDIDATES = (
    {'name':'prevalence','kind':'baseline'},
    {'name':'logistic_c01','kind':'logistic','C':0.1},
    {'name':'logistic_c1','kind':'logistic','C':1.0},
    {'name':'forest_depth4','kind':'forest','max_depth':4,'n_estimators':64,'min_samples_leaf':30},
    {'name':'forest_depth8','kind':'forest','max_depth':8,'n_estimators':64,'min_samples_leaf':30},
)
THRESHOLDS = tuple(round(0.1+i*0.05,2) for i in range(17))


def instant(value):
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('Temporal boundaries need explicit UTC/offset')
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class TrainingConfig:
    gold: GoldConfig = field(default_factory=GoldConfig)
    validation_start: str = '2025-10-01T00:00:00Z'
    test_start: str = '2026-04-01T00:00:00Z'
    seed: int = 42
    max_training_rows: int = 50000  # Driver memory limit, not a business threshold.
    experiment_name: str = 'lakehouse_to_ai_late_delivery'
    registered_model_name: str = 'lakehouse_demo_late_delivery_acceptance'

    def __post_init__(self):
        if not instant(self.validation_start)<instant(self.test_start)<instant('2026-10-28T00:00:00Z'):
            raise ValueError('Require validation_start < test_start < fixed observation cutoff')
        if self.seed!=42 or self.max_training_rows<30000:
            raise ValueError('Use seed 42 and a row guard large enough for 30000 training records')
        for value in (self.experiment_name,self.registered_model_name):
            if not value or len(value)>255 or any(c in value for c in ('\n','\r','/','\\')):
                raise ValueError('Use a nonempty MLflow experiment/model name without path separators')


def feature_contract():
    return dict(raw_predictors=list(PREDICTORS),identifiers_excluded=['order_id','customer_id'],
                metadata_excluded=['is_late','outcome_available_at','_batch_id','_reference_date'],
                categorical=list(CATEGORICAL),numeric=list(NUMERIC),
                derivation={'promised_lead_days':'(promised_delivery_at_acceptance - accepted_at).total_seconds() / 86400'},
                raw_timestamps='UTC ISO strings at model boundary; only used to compute promised_lead_days',
                no_other_derived_variables=True,decision_rule='P(is_late=True) >= validation-selected threshold')
