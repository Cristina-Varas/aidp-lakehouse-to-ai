"""Deterministic, versioned source extracts; ingestion is a separate future stage."""
from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
from importlib.resources import files
import json
import math
from pathlib import Path
import random

VERSION = "1"
SOURCE = "FICTIONAL_ERP"
DATA_FILES = ("customers.csv", "orders.csv", "deliveries.csv", "events.jsonl")
FEATURES = ["customer_segment_at_acceptance", "customer_region_at_acceptance",
            "priority_at_acceptance", "inventory_fill_rate_at_acceptance",
            "warehouse_load_at_acceptance", "planned_carrier_at_acceptance",
            "transit_distance_km", "line_count", "units", "promised_delivery_at_acceptance",
            "accepted_at"]
LATER_FIELDS = ["current_status", "current_priority", "source_updated_at", "row_version",
                "dispatched_at", "delivered_at", "event_type", "event_at"]


def stamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_stamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Source timestamps must include a timezone")
    return dt


def json_write(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class Config:
    seed: int = 42
    reference_date: date = date(2026, 10, 28)
    historical_orders: int = 30_000
    open_orders: int = 600

    def validate(self):
        if self.historical_orders < 100 or self.open_orders < 10:
            raise ValueError("Use at least 100 historical and 10 open orders")


def customer(number: int, at: datetime, rng: random.Random) -> dict:
    return dict(customer_id=f"C-{number:06d}", customer_name=f"Example Operations {number:04d}",
                region=rng.choice(["NORTH", "SOUTH", "EAST", "WEST"]),
                segment=rng.choice(["STANDARD", "STANDARD", "PREMIUM"]),
                source_created_at=stamp(at), source_updated_at=stamp(at), row_version=1)


def order(number: int, cust: dict, accepted: datetime, closed: bool,
          rng: random.Random) -> tuple[dict, dict | None, list[dict]]:
    priority = rng.choices(["NORMAL", "HIGH", "URGENT"], weights=[65, 25, 10])[0]
    units = rng.randint(5, 400)
    fill = round(rng.uniform(0.45, 1), 3)
    load = round(rng.uniform(0.35, 1.3), 3)
    distance = rng.randint(50, 2400)
    lead = rng.randint(4, 12)
    promised = accepted + timedelta(days=lead)
    carrier = rng.choice(["CARRIER_A", "CARRIER_B", "CARRIER_C"])
    result = dict(order_id=f"O-{number:08d}", customer_id=cust["customer_id"],
                  accepted_at=stamp(accepted), promised_delivery_at_acceptance=stamp(promised),
                  customer_segment_at_acceptance=cust["segment"],
                  customer_region_at_acceptance=cust["region"], priority_at_acceptance=priority,
                  inventory_fill_rate_at_acceptance=fill, warehouse_load_at_acceptance=load,
                  planned_carrier_at_acceptance=carrier, transit_distance_km=distance,
                  line_count=rng.randint(1, 12), units=units, current_priority=priority,
                  current_status="OPEN", source_created_at=stamp(accepted-timedelta(hours=2)),
                  source_updated_at=stamp(accepted), row_version=1)
    events = [event(result, "ACCEPTED", accepted)]
    delivery = None
    if closed:
        # Stochastic Bernoulli outcome with overlapping cohorts and unobserved disruption.
        logit = (-2.3 + 1.6*(1-fill) + 1.1*(load-.7) + .55*(units/400)
                 + .6*(distance/2400) + .8*(lead<=5) + .3*(carrier=="CARRIER_C")
                 - .25*(priority=="URGENT") + rng.gauss(0, .8))
        late = rng.random() < 1/(1+math.exp(-logit))
        delivered = promised + timedelta(days=rng.randint(1, 8) if late else -rng.randint(0, 2))
        delivery, later = complete(result, delivered, rng)
        events.extend(later)
        if late and rng.random() < .4:
            events.append(event(result, "INCIDENT_OPENED", parse_stamp(delivery["dispatched_at"]),
                                {"exception_type": rng.choice(["CAPACITY", "STOCK", "TRANSPORT"])}))
    return result, delivery, events


def event(row: dict, kind: str, at: datetime, payload: dict | None = None) -> dict:
    return dict(event_id=f"E-{row['order_id']}-{kind}", order_id=row["order_id"],
                event_type=kind, event_at=stamp(at), source_created_at=stamp(at),
                source_updated_at=stamp(at), row_version=1, payload=payload or {})


def complete(row: dict, delivered: datetime, rng: random.Random) -> tuple[dict, list[dict]]:
    dispatched = max(parse_stamp(row["accepted_at"])+timedelta(hours=12),
                     delivered-timedelta(days=rng.randint(1, 3)))
    updated = delivered+timedelta(hours=1)
    row.update(current_status="DELIVERED", source_updated_at=stamp(updated))
    delivery = dict(delivery_id="D-"+row["order_id"], order_id=row["order_id"],
                    dispatched_at=stamp(dispatched), delivered_at=stamp(delivered),
                    actual_carrier=row["planned_carrier_at_acceptance"],
                    source_created_at=stamp(dispatched), source_updated_at=stamp(updated), row_version=1)
    return delivery, [event(row, "DISPATCHED", dispatched), event(row, "DELIVERED", delivered)]


def write_batch(directory: Path, tables: dict, cut: datetime, config: Config,
                defects: list, changes: dict, expected: dict) -> dict:
    directory.mkdir()
    counts, hashes = {}, {}
    for name, rows in tables.items():
        path = directory / name
        if name.endswith(".csv"):
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
        else:
            with path.open("w", encoding="utf-8", newline="\n") as stream:
                for row in rows:
                    stream.write(json.dumps(row, sort_keys=True, separators=(",", ":"))+"\n")
        counts[name], hashes[name] = len(rows), digest(path)
    metadata = dict(batch_id=directory.name, source_system=SOURCE, extracted_at=stamp(cut),
                    files={n: dict(rows=counts[n], sha256=hashes[n]) for n in DATA_FILES},
                    ingestion_note="Actual ingestion time, object URI and run ID are assigned by future Bronze ingestion.")
    json_write(directory/"ingestion_metadata.json", metadata)
    manifest = dict(generator_version=VERSION, config={**asdict(config), "reference_date":str(config.reference_date)},
                    batch_id=directory.name, extracted_at=stamp(cut), row_counts=counts,
                    expected=expected, changes=changes, defects=defects,
                    defect_record_number="1-based data record, excluding CSV header; JSONL line number",
                    sha256=hashes)
    json_write(directory/"manifest.json", manifest)
    return manifest


def generate(config: Config, output: Path) -> dict:
    config.validate()
    output = output.expanduser().resolve()
    repo = Path(__file__).resolve().parents[2]
    if output == repo or repo in output.parents:
        raise ValueError("Generated datasets must be outside the repository")
    if output.exists():
        raise ValueError("Output already exists; choose a new directory (no data is overwritten)")
    rng = random.Random(config.seed)
    ref = datetime.combine(config.reference_date, time(0), tzinfo=timezone.utc)
    initial_cut, incremental_cut = ref, ref+timedelta(days=30)
    nc = max(20, min(600, config.historical_orders//10))
    customers = [customer(i, ref-timedelta(days=900), rng) for i in range(1, nc+1)]
    orders, deliveries, events = [], [], []
    for i in range(1, config.historical_orders+config.open_orders+1):
        closed = i<=config.historical_orders
        accepted = ref-timedelta(days=rng.randint(30, 730) if closed else rng.randint(1,5),
                                 hours=rng.randint(0, 23), minutes=rng.randint(0,59))
        row, delivery, ev = order(i, rng.choice(customers), accepted, closed, rng)
        if not closed:
            row["promised_delivery_at_acceptance"] = stamp(ref+timedelta(days=rng.randint(2,8)))
        orders.append(row)
        if delivery: deliveries.append(delivery)
        events.extend(ev)
    output.mkdir(parents=True)
    initial = write_batch(output/"initial", dict(zip(DATA_FILES,[customers,orders,deliveries,events])),
                          initial_cut, config, [], {},
                          dict(historical_orders=config.historical_orders, open_orders=config.open_orders,
                               valid_unique_orders=len(orders), quarantined_order_rows=0, duplicate_rows=0))
    new_count = max(40, config.historical_orders//30)
    update_count = min(120, config.open_orders)
    new_customers = [customer(nc+i, ref+timedelta(hours=8), rng) for i in range(1,21)]
    updated_customers = []
    for old in customers[:10]:
        updated_customers.append({**old, "segment":"PREMIUM" if old["segment"]=="STANDARD" else "STANDARD",
                                  "source_updated_at":stamp(ref+timedelta(days=1)), "row_version":2})
    inc_customers = new_customers+updated_customers
    inc_orders, inc_deliveries, inc_events = [], [], []
    for i in range(1,new_count+1):
        # Latest customer master is used only for orders accepted after the update.
        customer_pool = updated_customers+customers[10:]+new_customers
        row, delivery, ev = order(len(orders)+i, rng.choice(customer_pool),
                                  ref+timedelta(days=2,hours=rng.randint(0,23)),
                                  i<=int(new_count*.7), rng)
        inc_orders.append(row)
        if delivery: inc_deliveries.append(delivery)
        inc_events.extend(ev)
    updated_ids = []
    for old in orders[config.historical_orders:config.historical_orders+update_count]:
        updated = {**old, "row_version":2, "current_priority":"HIGH"}
        promised = parse_stamp(updated["promised_delivery_at_acceptance"])
        delivery, ev = complete(updated, promised+timedelta(days=rng.randint(-1,6)), rng)
        inc_orders.append(updated)
        inc_deliveries.append(delivery)
        inc_events.extend(ev)
        updated_ids.append(updated["order_id"])
    changes = dict(new_order_ids=[r["order_id"] for r in inc_orders[:new_count]],
                   updated_order_ids=updated_ids,
                   new_customer_ids=[r["customer_id"] for r in new_customers],
                   updated_customer_ids=[r["customer_id"] for r in updated_customers],
                   update_semantics="Higher row_version/source_updated_at wins; acceptance snapshots are immutable.")
    inc_tables = dict(zip(DATA_FILES,[inc_customers,inc_orders,inc_deliveries,inc_events]))
    defects = []
    for name, count in [("customers.csv",5),("orders.csv",25),("deliveries.csv",10),("events.jsonl",20)]:
        key = {"customers.csv":"customer_id","orders.csv":"order_id","deliveries.csv":"delivery_id","events.jsonl":"event_id"}[name]
        for row in list(inc_tables[name][:count]):
            inc_tables[name].append(dict(row))
            defects.append(dict(file=name, record_number=len(inc_tables[name]), kind="EXACT_DUPLICATE",
                                business_key=row[key], expected_action="DEDUPLICATE"))
    for kind, count in [("INVALID_DATE",8),("UNKNOWN_CUSTOMER",7)]:
        for i in range(1,count+1):
            row = {**inc_orders[0], "order_id":f"O-{kind}-{i:03d}", "current_status":"OPEN"}
            row["source_updated_at"] = row["accepted_at"]
            if kind=="INVALID_DATE": row["accepted_at"]=f"{config.reference_date.year}-02-30T00:00:00Z"
            else: row["customer_id"]="C-UNKNOWN"
            inc_orders.append(row)
            defects.append(dict(file="orders.csv", record_number=len(inc_orders), kind=kind,
                                business_key=row["order_id"], expected_action="QUARANTINE"))
    incremental = write_batch(output/"incremental", inc_tables, incremental_cut, config, defects, changes,
                              dict(new_orders=new_count, updated_orders=update_count,
                                   valid_unique_orders=new_count+update_count,
                                   new_customers=20, updated_customers=10,
                                   invalid_date_rows=8, unknown_customer_rows=7,
                                   quarantined_order_rows=15, duplicate_rows=60,
                                   closed_new_orders=int(new_count*.7)))
    documents=output/"documents"
    documents.mkdir()
    for name in ("order_prioritization.md","incident_escalation.md"):
        documents.joinpath(name).write_text(files("aidp_lakehouse").joinpath("policies",name).read_text(encoding="utf-8"),encoding="utf-8")
    summary=dict(generator_version=VERSION, initial=initial["row_counts"], incremental=incremental["row_counts"],
                 prediction_time="accepted_at", acceptance_features=FEATURES, excluded_later_fields=LATER_FIELDS,
                 target="delivered_at > promised_delivery_at_acceptance; only observed completed deliveries have labels",
                 documents={p.name:digest(p) for p in sorted(documents.iterdir())})
    json_write(output/"dataset_manifest.json",summary)
    return summary
