"""Local contract/guard checks, not a substitute for executing AIDP SQL."""
import ast
import csv
from datetime import date
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from aidp_lakehouse.aidp_bronze import (
    BronzeConfig, CSV_COLUMNS, ENTITIES, columns, catalog_ddl, ensure_catalog,
    inspect_table, reader_options, register_bronze, table_ddl, validate_control_documents,
)
from aidp_lakehouse.generator import Config, generate


class Row(dict):
    def asDict(self):
        return dict(self)


class Frame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class FakeAidp:
    """Records metadata operations only; does not parse SQL or run Spark."""
    def __init__(self):
        self.statements = []
        self.tables = {}
        self.catalog_type = "Standard Catalog"

    def sql(self, sql):
        self.statements.append(sql)
        if sql.startswith("DESCRIBE CATALOG"):
            return Frame([("Name", "lakehouse_demo"), ("Type", self.catalog_type)])
        if sql.startswith("SHOW TABLES"):
            return Frame([Row(tableName=key) for key in self.tables])
        if sql.startswith("CREATE EXTERNAL TABLE"):
            entity = sql.split()[6].split(".")[-1]
            opts = ", ".join(f"{key}={value}" for key, value in reader_options(entity).items())
            self.tables[entity] = dict(type="EXTERNAL", provider="json" if entity == "events" else "csv",
                                       location=BronzeConfig().location(entity), **{"storage properties": f"[{opts}]"})
        if sql.startswith("DESCRIBE TABLE EXTENDED"):
            return Frame(list(self.tables[sql.split()[-1].split(".")[-1]].items()))
        return Frame([])

    def table(self, name):
        entity = name.split(".")[-1]
        fields = [SimpleNamespace(name=key, dataType=SimpleNamespace(simpleString=lambda dtype=dtype: dtype))
                  for key, dtype in columns(entity)]
        return SimpleNamespace(schema=SimpleNamespace(fields=fields))


class AidpBronzeTests(unittest.TestCase):
    def test_ddl_scope_and_raw_contract(self):
        cfg = BronzeConfig()
        self.assertEqual(catalog_ddl(cfg), [
            "CREATE CATALOG IF NOT EXISTS lakehouse_demo",
            "CREATE SCHEMA IF NOT EXISTS lakehouse_demo.bronze",
            "CREATE SCHEMA IF NOT EXISTS lakehouse_demo.silver",
            "CREATE SCHEMA IF NOT EXISTS lakehouse_demo.gold",
        ])
        for entity in ENTITIES:
            sql = table_ddl(cfg, entity)
            self.assertTrue(sql.startswith(f"CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_demo.bronze.{entity}"))
            self.assertIn(f"/{entity}/initial/", sql)
            self.assertNotIn("/incremental/", sql)
            self.assertNotIn("/_control/", sql)
            self.assertNotIn("/documents/", sql)
            self.assertIn("mode = 'PERMISSIVE'", sql)
            self.assertIn("recursiveFileLookup = 'false'", sql)
            self.assertIn("source_created_at STRING", sql)
            self.assertIn("source_updated_at STRING", sql)
            self.assertNotIn("TIMESTAMP", sql)
            self.assertNotIn("DROPMALFORMED", sql)
            if entity != "events":
                self.assertTrue(all(dtype == "string" for _, dtype in columns(entity)))
        self.assertIn("payload MAP<STRING,STRING>", table_ddl(cfg, "events"))
        self.assertIn("row_version BIGINT", table_ddl(cfg, "events"))

    def test_existing_tables_preserved_and_conflict_stops_all_creates(self):
        spark, cfg = FakeAidp(), BronzeConfig()
        register_bronze(spark, cfg)
        self.assertEqual(set(spark.tables), set(ENTITIES))
        spark.statements.clear()
        result = register_bronze(spark, cfg)
        self.assertTrue(all(row["action"] == "PRESERVED" for row in result))
        self.assertFalse(any(s.startswith("CREATE") for s in spark.statements))
        spark.tables.pop("events")
        spark.tables["orders"]["location"] = cfg.root_uri + "/orders/incremental/"
        spark.statements.clear()
        with self.assertRaisesRegex(ValueError, "differs"):
            register_bronze(spark, cfg)
        self.assertFalse(any(s.startswith("CREATE") for s in spark.statements))

    def test_rejects_external_catalog_before_creating_schemas(self):
        spark = FakeAidp()
        spark.catalog_type = "External Catalog"
        with self.assertRaisesRegex(ValueError, "Standard"):
            ensure_catalog(spark, BronzeConfig())
        self.assertFalse(any(s.startswith("CREATE SCHEMA") for s in spark.statements))

    def test_existing_dropmalformed_parser_not_accepted(self):
        spark, cfg = FakeAidp(), BronzeConfig()
        register_bronze(spark, cfg)
        spark.tables["orders"]["storage properties"] = "[mode=DROPMALFORMED, header=true]"
        with self.assertRaisesRegex(ValueError, "reader option"):
            inspect_table(spark, cfg, "orders")

    def test_fixed_batch_and_literal_locations(self):
        for change in [dict(reference_date="2026-10-29"), dict(batch_id="incremental"),
                       dict(catalog="lakehouse_demo;DROP"), dict(prefix="root/*"), dict(prefix="root/../data")]:
            with self.assertRaises(ValueError):
                BronzeConfig(**change)
        self.assertIn("@frgpslcqn7sm/", BronzeConfig().location("orders"))
        self.assertNotIn("@oraemeaaidata2/", BronzeConfig().location("orders"))

    def test_generated_files_match_raw_schema_and_control_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "source"
            generate(Config(42, date(2026, 10, 28), 100, 10), root)
            for entity, expected in CSV_COLUMNS.items():
                with (root / "initial" / f"{entity}.csv").open(newline="") as stream:
                    self.assertEqual(csv.DictReader(stream).fieldnames, expected)
            manifest = json.loads((root / "initial/manifest.json").read_text())
            metadata = json.loads((root / "initial/ingestion_metadata.json").read_text())
            validate_control_documents(BronzeConfig(), manifest, metadata)
            with (root / "initial/events.jsonl").open() as stream:
                events = [json.loads(line) for line in stream]
            self.assertTrue(all(isinstance(e["payload"], dict) and isinstance(e["row_version"], int) for e in events))
            self.assertTrue(all(isinstance(v, str) for e in events for v in e["payload"].values()))
            with (root / "incremental/orders.csv").open(newline="") as stream:
                invalid = [r for r in csv.DictReader(stream) if "INVALID_DATE" in r["order_id"]]
            self.assertEqual(len(invalid), 8)
            self.assertTrue(all(r["accepted_at"] == "2026-02-30T00:00:00Z" for r in invalid))
            self.assertEqual(dict(columns("orders"))["accepted_at"], "string")
            metadata["files"]["orders.csv"]["rows"] += 1
            with self.assertRaisesRegex(ValueError, "disagree"):
                validate_control_documents(BronzeConfig(), manifest, metadata)

    def test_notebooks_are_importable_unexecuted_and_share_configuration(self):
        try:
            import nbformat
        except ImportError:
            self.skipTest("Install .[notebooks] to run full notebook-format validation")
        repo = Path(__file__).resolve().parents[1]
        configs = []
        for name in ("00_setup_catalog", "01_register_bronze"):
            n = nbformat.read(repo / "notebooks" / f"{name}.ipynb", as_version=4)
            nbformat.validate(n)
            self.assertEqual(n.metadata.aidp.runtime_verification, "pending")
            code = [c for c in n.cells if c.cell_type == "code"]
            for cell in code:
                ast.parse(cell.source)
                self.assertIsNone(cell.execution_count)
                self.assertEqual(cell.outputs, [])
                self.assertNotIn("CREATE OR REPLACE", cell.source.upper())
            scope = {}
            exec(code[0].source, scope)
            configs.append(scope["CONFIG"])
        self.assertEqual(configs[0], configs[1])
        self.assertEqual(BronzeConfig(**configs[0]).reference_date, "2026-10-28")


if __name__ == "__main__":
    unittest.main()
