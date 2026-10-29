"""Local contract/guard checks, not a substitute for executing AIDP SQL."""
import ast
import csv
from datetime import date
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call

from aidp_lakehouse.aidp_bronze import (
    BronzeConfig, CSV_COLUMNS, ENTITIES, columns, catalog_ddl, ensure_catalog,
    inspect_table, read_control_documents, reader_options, register_bronze, require_standard_catalog,
    table_ddl, validate_control_documents,
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


OBSERVED_CATALOG_ROWS = [
    {"attribute": "name", "attribute_value": "lakehouse_demo"},
    {"attribute": "type", "attribute_value": "INTERNAL"},
    {"attribute": "catalog_details", "attribute_value": '{"catalogType":"INTERNAL"}'},
]


class ObservedCatalogAidp:
    """Existing metadata with the user's exact DESCRIBE column names and values."""
    def __init__(self, rows=None):
        self.rows = OBSERVED_CATALOG_ROWS if rows is None else rows
        self.statements = []
        self.catalog = {"name": "lakehouse_demo", "type": "INTERNAL", "description": "Existing catalog"}
        self.schemas = {name: {"description": "Existing schema"} for name in ("bronze", "silver", "gold")}

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("DESCRIBE CATALOG"):
            return Frame(self.rows)
        if statement == "CREATE CATALOG IF NOT EXISTS lakehouse_demo":
            return Frame([])  # Catalog already exists: no change.
        if statement.startswith("CREATE SCHEMA IF NOT EXISTS lakehouse_demo."):
            self.schemas.setdefault(statement.rsplit(".", 1)[1], {})
            return Frame([])
        raise AssertionError("Unexpected or destructive SQL")


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
    def test_control_json_uses_direct_wholetext_argument(self):
        cfg = BronzeConfig()
        names = [cfg.file_uri(entity).rsplit("/", 1)[1] for entity in ENTITIES]
        manifest = dict(batch_id="initial", config=dict(reference_date="2026-10-28", seed=42),
                        defects=[], extracted_at="2026-10-28T00:00:00Z",
                        row_counts={name: 10 for name in names}, sha256={name: "a" * 64 for name in names})
        metadata = dict(batch_id="initial", extracted_at=manifest["extracted_at"],
                        files={name: dict(rows=10, sha256="a" * 64) for name in names})
        documents = {cfg.control_uri("manifest.json"): manifest,
                     cfg.control_uri("ingestion_metadata.json"): metadata}
        reader = Mock(spec=["text"])  # No .option() API: regression must use .text's keyword.

        def text(uri, *, wholetext):
            self.assertIs(wholetext, True)
            return Frame([Row(value=json.dumps(documents[uri], indent=2))])

        reader.text.side_effect = text
        spark = SimpleNamespace(read=reader)
        self.assertEqual(read_control_documents(spark, cfg), (manifest, metadata))
        self.assertEqual(reader.text.call_args_list, [
            call(cfg.control_uri("manifest.json"), wholetext=True),
            call(cfg.control_uri("ingestion_metadata.json"), wholetext=True),
        ])
        reader.text.side_effect = None
        reader.text.return_value = Frame([])
        with self.assertRaisesRegex(ValueError, "one complete control document"):
            read_control_documents(spark, cfg)

    def test_reader_options_accept_observed_table_properties_and_storage_properties(self):
        cfg = BronzeConfig()
        cases = [
            {"Storage Properties": "[mode=PERMISSIVE, multiLine=false, header=true, inferSchema=false]"},
            {"Table Properties": "[mode=PERMISSIVE, option.mode=PERMISSIVE, multiLine=false, header=true, inferSchema=false]"},
            {"Table Properties": "[option.mode=PERMISSIVE, option.multiLine=false, option.header=true, option.inferSchema=false]"},
            {"Storage Properties": "[mode=permissive, multiLine=FALSE]",
             "Table Properties": "[option.mode=PERMISSIVE, header=TRUE, inferSchema=false]"},
        ]
        for properties in cases:
            with self.subTest(properties=properties):
                spark = FakeAidp()
                register_bronze(spark, cfg)
                for entity in ENTITIES:
                    spark.tables[entity].pop("storage properties")
                    spark.tables[entity].update(properties)
                spark.statements.clear()
                result = register_bronze(spark, cfg)
                self.assertTrue(all(row["action"] == "PRESERVED" for row in result))
                self.assertFalse(any(sql.startswith("CREATE") for sql in spark.statements))

    def test_reader_options_reject_conflicts_across_sections_aliases_and_duplicates(self):
        cfg = BronzeConfig()
        required = {"mode": "PERMISSIVE", "multiLine": "false", "header": "true", "inferSchema": "false"}
        base = "[" + ", ".join(f"{key}={value}" for key, value in required.items()) + "]"
        cases = []
        for key, bad in dict(mode="DROPMALFORMED", multiLine="true", header="false", inferSchema="true").items():
            # Both orderings must reject a disagreement, even when a good assertion is present.
            for first, second in ((required[key], bad), (bad, required[key])):
                cases.extend([
                    [("Table Properties", base[:-1] + f", {key}={first}, option.{key}={second}]")],
                    [("Storage Properties", base), ("Table Properties", f"[option.{key}={bad}]")],
                    [("Table Properties", base), ("Table Properties", f"[{key}={bad}]")],
                ])
        # Conflicting non-required reader options must also be rejected.
        cases.append([("Table Properties", base[:-1] + ", recursiveFileLookup=false, option.recursiveFileLookup=true]")])
        for properties in cases:
            with self.subTest(properties=properties):
                spark = FakeAidp()
                register_bronze(spark, cfg)
                details = [(key, value) for key, value in spark.tables["orders"].items() if key != "storage properties"]
                spark.sql = lambda sql: Frame(details + properties)
                with self.assertRaisesRegex(ValueError, "Conflicting reader option"):
                    inspect_table(spark, cfg, "orders")

    def test_reader_properties_do_not_bypass_existing_table_checks(self):
        cfg = BronzeConfig()
        observed = "[mode=PERMISSIVE, option.mode=PERMISSIVE, multiLine=false, header=true, inferSchema=false]"
        for key, bad in dict(type="MANAGED", provider="parquet", location=cfg.root_uri + "/orders/incremental/").items():
            with self.subTest(key=key):
                spark = FakeAidp()
                register_bronze(spark, cfg)
                spark.tables["orders"].pop("storage properties")
                spark.tables["orders"].update({"Table Properties": observed, key: bad})
                with self.assertRaisesRegex(ValueError, "type/provider/location differs"):
                    inspect_table(spark, cfg, "orders")
        spark = FakeAidp()
        register_bronze(spark, cfg)
        spark.tables["orders"].pop("storage properties")
        spark.tables["orders"]["Table Properties"] = observed
        original_table = spark.table
        spark.table = lambda name: SimpleNamespace(schema=SimpleNamespace(fields=original_table(name).schema.fields[:-1]))
        with self.assertRaisesRegex(ValueError, "column schema differs"):
            inspect_table(spark, cfg, "orders")
        for missing in ("mode", "multiLine", "header", "inferSchema"):
            spark = FakeAidp()
            register_bronze(spark, cfg)
            spark.tables["orders"].pop("storage properties")
            options = {key: value for key, value in reader_options("orders").items() if key != missing}
            spark.tables["orders"]["Table Properties"] = "[" + ", ".join(f"{k}={v}" for k,v in options.items()) + "]"
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, "Cannot confirm"):
                inspect_table(spark, cfg, "orders")

    def test_observed_internal_attribute_rows_are_standard(self):
        spark = ObservedCatalogAidp()
        self.assertIs(require_standard_catalog(spark, BronzeConfig()), OBSERVED_CATALOG_ROWS)
        # PySpark Row exposes named columns via asDict, not dictionary iteration.
        spark.rows = [Row(row) for row in OBSERVED_CATALOG_ROWS]
        require_standard_catalog(spark, BronzeConfig())

    def test_catalog_type_and_attribute_normalization_preserves_supported_types(self):
        for value in ("Standard Catalog", "STANDARD_CATALOG", " standard  catalog ",
                      "standard", "CATALOG", "INTERNAL", " internal "):
            with self.subTest(value=value):
                require_standard_catalog(ObservedCatalogAidp([
                    {" ATTRIBUTE ": " Catalog_Type ", " ATTRIBUTE_VALUE ": value},
                ]), BronzeConfig())
                require_standard_catalog(ObservedCatalogAidp([(" Catalog Type ", value)]), BronzeConfig())
        require_standard_catalog(ObservedCatalogAidp([
            {"attribute": "TYPE", "attribute_value": "Standard Catalog"},
            {"attribute": "catalog-details", "attribute_value": '{"catalogType":"internal"}'},
        ]), BronzeConfig())
        require_standard_catalog(ObservedCatalogAidp([
            {"attribute": "catalog_details", "attribute_value": '{"catalogType":"INTERNAL"}'},
        ]), BronzeConfig())

    def test_catalog_external_unknown_missing_and_conflicting_types_fail_closed(self):
        cases = [[], [("name", "lakehouse_demo")]]
        for value in (None, "", " ", "UNKNOWN", "EXTERNAL", "External Catalog", "ORACLE_ADW"):
            cases.append([{"attribute": "type", "attribute_value": value}])
            cases.append([
                {"attribute": "type", "attribute_value": "INTERNAL"},
                {"attribute": "catalog_details", "attribute_value": json.dumps({"catalogType": value})},
            ])
        for left, right in (("INTERNAL", "EXTERNAL"), ("EXTERNAL", "INTERNAL"), ("UNKNOWN", "INTERNAL")):
            cases.append([("type", left), ("catalog_type", right)])
            cases.append([{"attribute": "type", "attribute_value": value} for value in (left, right)])
            cases.append([{"attribute": "catalog_details", "attribute_value":
                           '{"catalogType":"' + left + '","catalogType":"' + right + '"}'}])
        for value in ("not json", "null", "[]", "{}", '{"name":"lakehouse_demo"}'):
            cases.append([
                {"attribute": "type", "attribute_value": "INTERNAL"},
                {"attribute": "catalog_details", "attribute_value": value},
            ])
        for rows in cases:
            with self.subTest(rows=rows):
                spark = ObservedCatalogAidp(rows)
                with self.assertRaises(ValueError):
                    ensure_catalog(spark, BronzeConfig())
                self.assertFalse(any(s.startswith("CREATE SCHEMA") for s in spark.statements))

    def test_setup_rerun_preserves_existing_internal_catalog_and_schemas(self):
        import copy
        spark = ObservedCatalogAidp()
        before = copy.deepcopy((spark.catalog, spark.schemas))
        for _ in range(2):
            ensure_catalog(spark, BronzeConfig())
        self.assertEqual((spark.catalog, spark.schemas), before)
        creates = [s for s in spark.statements if s.startswith("CREATE")]
        self.assertEqual(len(creates), 8)
        self.assertTrue(all("IF NOT EXISTS" in s for s in creates))
        self.assertEqual(sum(s.startswith("DESCRIBE CATALOG") for s in spark.statements), 2)

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
