"""Canonical notebook orchestration. Registration never removes existing metadata."""
from functools import wraps
import json
from . import __version__
from .aidp_bronze import (BronzeConfig, ENTITIES, ensure_catalog, read_control_documents,
                         register_bronze, _details, provenance_sample_sql)
from .bronze_validation import compare_table


def stage(function):
    @wraps(function)
    def guarded(self, *args, **kwargs):
        try:
            return function(self, *args, **kwargs)
        except Exception as error:
            self.failed = True
            print(f'BRONZE FAIL [{function.__name__}]: {type(error).__name__}: {error}')
            raise
    return guarded


class BronzeRun:
    def __init__(self, spark, config):
        self.spark = spark
        self.failed = False
        self.manifest = None
        self.verification = None
        try:
            self.config = BronzeConfig(**config)
            if __version__ not in ('0.2.2', '0.3.0'):
                raise ValueError('Install aidp_lakehouse_to_ai-0.3.0-py3-none-any.whl and restart the session')
            if '/source-data/v2/' not in self.config.prefix:
                raise ValueError('Canonical Bronze workflow requires the validated LF v2 sources')
            if not hasattr(spark, 'sparkContext'):
                raise ValueError('Attach authenticated AIDP Spark compute')
            print('Spark:', spark.version, 'Package:', __version__)
            print('Configuration:', json.dumps(config, sort_keys=True))
        except Exception as error:
            self.failed = True
            print(f'BRONZE FAIL [runtime/configuration]: {error}')
            raise

    @stage
    def setup(self):
        ensure_catalog(self.spark, self.config)
        print('Standard catalog and bronze/silver/gold schemas verified.')

    @stage
    def controls(self):
        self.manifest, self.ingestion_metadata = read_control_documents(self.spark, self.config)
        print('Validated control files:', self.config.control_uri('manifest.json'),
              self.config.control_uri('ingestion_metadata.json'))
        print('Expected rows:', json.dumps(self.manifest['row_counts'], sort_keys=True))
        print('Batch:', self.manifest['batch_id'], 'Reference date:', self.config.reference_date,
              'Extracted at:', self.manifest['extracted_at'])

    @stage
    def register(self):
        if self.manifest is None:
            raise ValueError('Validate control documents before registration')
        # Read-only preflight ALL existing canonical registrations before creating any.
        visible = {row.asDict()['tableName'] for row in self.spark.sql(
            f'SHOW TABLES IN {self.config.catalog}.bronze').collect()}
        for entity in ENTITIES:
            if entity not in visible:
                continue
            rows = self.spark.sql(f'DESCRIBE TABLE EXTENDED {self.config.table(entity)}').collect()
            location = _details(rows).get('location', '')
            if '/source-data/v1/' in location:
                raise ValueError(f'{self.config.table(entity)} still points to v1. Stop: run the separate '
                                 'reviewed migration in notebooks/migrations/migrate_bronze_v2.py; '
                                 'see docs/bronze-v2-migration.md. This notebook preserves existing tables.')
        self.registration = register_bronze(self.spark, self.config)
        print(json.dumps(self.registration, indent=2))

    @stage
    def verify(self):
        if self.manifest is None:
            raise ValueError('Control documents have not been validated')
        results = []
        for entity in ENTITIES:
            count = self.manifest['row_counts'][self.config.file_uri(entity).rsplit('/', 1)[1]]
            result = compare_table(self.spark, self.config, entity, expected_count=count)
            results.append(result)
            print(json.dumps(result, default=str))
        self.verification = results

    @stage
    def samples(self):
        if self.verification is None:
            raise ValueError('Complete verification before samples')
        for entity in ENTITIES:
            print('Sample:', self.config.table(entity))
            self.spark.table(self.config.table(entity)).limit(3).show(truncate=False)
            self.spark.sql(provenance_sample_sql(self.config, entity,
                                                self.manifest['extracted_at'])).show(truncate=False)

    @stage
    def finish(self):
        if self.failed or self.verification is None or len(self.verification) != len(ENTITIES):
            raise ValueError('Verification is incomplete or an earlier stage failed; restart from the top')
        print('BRONZE PASS: all four external tables match direct v2 reads in schema, all rows/columns, '
              'duplicate multiplicity, exact provenance and zero corrupt rows.')
        print('Next: notebooks/02_build_silver.ipynb (same v2 configuration). Silver live success is user-reported; see docs/aidp-silver-execution.md.')
        return self.verification
