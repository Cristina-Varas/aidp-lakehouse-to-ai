"""Explicit initial Gold contracts and prediction roles; import without Spark."""
from dataclasses import dataclass, field
from .aidp_bronze import BronzeConfig, identifier
from .silver import CONTRACTS

VERSION = '0.3.0'
V2_PREFIX = 'demos/lakehouse-to-ai/source-data/v2/seed-42/reference-2026-10-28'
IDENTIFIERS = ('order_id', 'customer_id')
# Original, immutable acceptance attributes ONLY. Never infer features from a schema.
PREDICTORS = (
    'customer_segment_at_acceptance', 'customer_region_at_acceptance',
    'priority_at_acceptance', 'inventory_fill_rate_at_acceptance',
    'warehouse_load_at_acceptance', 'planned_carrier_at_acceptance',
    'transit_distance_km', 'line_count', 'units',
    'promised_delivery_at_acceptance', 'accepted_at',
)
LABEL = 'is_late'
TRAINING_METADATA = ('outcome_available_at', '_batch_id', '_reference_date')
SCORING_METADATA = ('_batch_id', '_reference_date')
EXPECTED_SILVER = {'customers':600, 'orders':30600, 'deliveries':30000, 'events':94572}
ORDER_RENAMES = {'source_created_at':'order_source_created_at',
                 'source_updated_at':'order_source_updated_at', 'row_version':'order_row_version'}
DELIVERY_RENAMES = {'source_created_at':'delivery_source_created_at',
                    'source_updated_at':'delivery_source_updated_at', 'row_version':'delivery_row_version'}
OPERATIONS = [(ORDER_RENAMES.get(n,n),t) for n,t in CONTRACTS['orders']] + [
    ('order_source_file','string'), ('customer_name','string'),
    ('customer_region_current','string'), ('customer_segment_current','string'),
] + [(DELIVERY_RENAMES.get(n,n),t) for n,t in CONTRACTS['deliveries'] if n!='order_id'] + [
    ('delivery_source_file','string'), ('event_count','bigint'), ('incident_count','bigint'),
    ('incident_types','array<string>'), ('first_incident_at','timestamp'),
    ('last_incident_at','timestamp'), ('last_event_at','timestamp'),
    (LABEL,'boolean'), ('outcome_available_at','timestamp'),
    ('_batch_id','string'), ('_reference_date','date'),
]
MODEL_BASE = [(n,dict(CONTRACTS['orders'])[n]) for n in (*IDENTIFIERS,*PREDICTORS)]
CONTRACTS_GOLD = {
    'order_operations': OPERATIONS,
    'delivery_kpis': [('period_start','date'), ('customer_region_at_acceptance','string'),
                     ('actual_carrier','string'), ('deliveries','bigint'),
                     ('late_deliveries','bigint'), ('late_delivery_pct','decimal(9,6)'),
                     ('_batch_id','string'), ('_reference_date','date')],
    'late_delivery_training': MODEL_BASE + [(LABEL,'boolean'),('outcome_available_at','timestamp'),
                                          ('_batch_id','string'),('_reference_date','date')],
    'late_delivery_scoring': MODEL_BASE + [('_batch_id','string'),('_reference_date','date')],
}
KEYS = {'order_operations':('order_id',), 'late_delivery_training':('order_id',),
        'late_delivery_scoring':('order_id',),
        'delivery_kpis':('period_start','customer_region_at_acceptance','actual_carrier')}
NULLABLE_OPERATIONS = {n for n,_ in CONTRACTS_GOLD['order_operations'] if n.startswith('delivery_')} | {
    'dispatched_at','delivered_at','actual_carrier',LABEL,'outcome_available_at',
    'first_incident_at','last_incident_at'}


@dataclass(frozen=True)
class GoldConfig:
    bronze: BronzeConfig = field(default_factory=lambda: BronzeConfig(prefix=V2_PREFIX))

    def __post_init__(self):
        # This initial milestone is deliberately fixed to the verified dataset.
        if self.bronze.prefix != V2_PREFIX or self.bronze.seed != 42:
            raise ValueError('Initial Gold requires seed 42 and the verified LF v2 prefix')
        identifier(self.bronze.catalog)

    @property
    def silver_namespace(self):
        return f'{self.bronze.catalog}.silver'

    @property
    def gold_namespace(self):
        return f'{self.bronze.catalog}.gold'
