"""Independent source-quality checks; validation does not clean or modify extracts."""
from __future__ import annotations

from collections import Counter
import csv
import json
from pathlib import Path

from .generator import DATA_FILES, FEATURES, digest, parse_stamp

KEYS = dict(zip(DATA_FILES, ["customer_id", "order_id", "delivery_id", "event_id"]))
TIMES = {"accepted_at", "promised_delivery_at_acceptance", "source_created_at",
         "source_updated_at", "event_at", "dispatched_at", "delivered_at"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_records(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream)) if path.suffix==".csv" else [json.loads(line) for line in stream]


def classify(tables: dict, known_customers: set[str]):
    valid, defects = {}, []
    for name, rows in tables.items():
        seen, clean = set(), []
        for number, row in enumerate(rows, 1):
            serialized = json.dumps(row,sort_keys=True)
            kind = None
            if serialized in seen:
                kind="EXACT_DUPLICATE"
            else:
                seen.add(serialized)
                try:
                    for field in TIMES.intersection(row): parse_stamp(row[field])
                except (ValueError, TypeError): kind="INVALID_DATE"
                if kind is None and name=="orders.csv" and row["customer_id"] not in known_customers:
                    kind="UNKNOWN_CUSTOMER"
            if kind:
                defects.append(dict(file=name,record_number=number,kind=kind,business_key=row[KEYS[name]],
                                    expected_action="DEDUPLICATE" if kind=="EXACT_DUPLICATE" else "QUARANTINE"))
            else: clean.append(row)
        valid[name]=clean
    return valid, defects


def unique(rows, key):
    result={r[key]:r for r in rows}
    require(len(result)==len(rows), f"Unexpected duplicate business key: {key}")
    require(all(result),f"Empty business key: {key}")
    return result


def validate_dataset(root: Path) -> dict:
    root=root.expanduser().resolve()
    manifests, batches = {}, {}
    known_customers=set()
    for batch in ("initial","incremental"):
        directory=root/batch
        m=json.loads((directory/"manifest.json").read_text())
        metadata=json.loads((directory/"ingestion_metadata.json").read_text())
        raw={name:read_records(directory/name) for name in DATA_FILES}
        require(m["batch_id"]==metadata["batch_id"]==batch,"Batch identity mismatch")
        require(metadata["extracted_at"]==m["extracted_at"],"Extraction time mismatch")
        known_customers.update(r["customer_id"] for r in raw["customers.csv"])
        clean, defects=classify(raw,known_customers)
        require(sorted(defects,key=lambda r:(r["file"],r["record_number"]))==
                sorted(m["defects"],key=lambda r:(r["file"],r["record_number"])),"Observed defects differ from manifest")
        for name,rows in raw.items():
            require(len(rows)==m["row_counts"][name]==metadata["files"][name]["rows"],"Row count mismatch: "+name)
            require(digest(directory/name)==m["sha256"][name]==metadata["files"][name]["sha256"],"Checksum mismatch: "+name)
            unique(clean[name],KEYS[name])
            for row in clean[name]:
                created,updated=parse_stamp(row["source_created_at"]),parse_stamp(row["source_updated_at"])
                require(created<=updated<=parse_stamp(m["extracted_at"]),"Source version occurs outside extraction snapshot")
                require(int(row["row_version"])>=1,"Invalid row version")
        manifests[batch],batches[batch]=m,clean
    initial,inc=batches["initial"],batches["incremental"]
    cfg=manifests["initial"]["config"]
    require(cfg==manifests["incremental"]["config"],"Batch configurations differ")
    hist,opened=cfg["historical_orders"],cfg["open_orders"]
    require(len(initial["orders.csv"])==hist+opened,"Initial order cohort mismatch")
    require(len(initial["deliveries.csv"])==hist,"Historical delivery count mismatch")
    require(not manifests["initial"]["defects"],"Initial batch must be clean")
    counts=Counter((d["file"],d["kind"]) for d in manifests["incremental"]["defects"])
    require(counts==Counter({("customers.csv","EXACT_DUPLICATE"):5,("orders.csv","EXACT_DUPLICATE"):25,
                            ("deliveries.csv","EXACT_DUPLICATE"):10,("events.jsonl","EXACT_DUPLICATE"):20,
                            ("orders.csv","INVALID_DATE"):8,("orders.csv","UNKNOWN_CUSTOMER"):7}),"Defect count contract differs")
    expected=manifests["incremental"]["expected"]
    require(expected==dict(new_orders=max(40,hist//30),updated_orders=min(120,opened),
                          valid_unique_orders=max(40,hist//30)+min(120,opened),new_customers=20,updated_customers=10,
                          invalid_date_rows=8,unknown_customer_rows=7,quarantined_order_rows=15,duplicate_rows=60,
                          closed_new_orders=int(max(40,hist//30)*.7)),"Incremental expected counts differ from contract")
    initial_expected=dict(historical_orders=hist,open_orders=opened,valid_unique_orders=hist+opened,
                          quarantined_order_rows=0,duplicate_rows=0)
    require(manifests["initial"]["expected"]==initial_expected,"Initial expected counts differ")
    current={name:unique(initial[name],KEYS[name]) for name in DATA_FILES}
    changes=manifests["incremental"]["changes"]
    for name in DATA_FILES:
        inserts,updates=[],[]
        for row in inc[name]:
            key=row[KEYS[name]]
            old=current[name].get(key)
            if old:
                require(name in ("customers.csv","orders.csv"),"Unexpected delivery/event update")
                require(int(row["row_version"])==int(old["row_version"])+1,"Version must advance exactly once")
                require(parse_stamp(row["source_updated_at"])>parse_stamp(old["source_updated_at"]),"Update timestamp must advance")
                require(row["source_created_at"]==old["source_created_at"],"Creation timestamp changed")
                if name=="orders.csv":
                    require(all(row[f]==old[f] for f in FEATURES+["customer_id"]),"Acceptance snapshot changed")
                    require(old["current_status"]=="OPEN" and row["current_status"]=="DELIVERED","Unexpected order transition")
                else: require(row["segment"]!=old["segment"],"Expected customer segment update missing")
                updates.append(key)
            else: inserts.append(key)
            current[name][key]=row
        if name in ("customers.csv","orders.csv"):
            noun="customer" if name=="customers.csv" else "order"
            require(sorted(inserts)==sorted(changes[f"new_{noun}_ids"]),"Insert manifest differs")
            require(sorted(updates)==sorted(changes[f"updated_{noun}_ids"]),"Update manifest differs")
            require(len(inserts)==expected[f"new_{noun}s"] and len(updates)==expected[f"updated_{noun}s"],"Change counts differ")
    orders=current["orders.csv"]
    deliveries={r["order_id"]:r for r in current["deliveries.csv"].values()}
    require(len(deliveries)==len(current["deliveries.csv"]),"More than one delivery per order")
    require(set(deliveries)<=set(orders),"Delivery references unknown order")
    initial_orders=unique(initial["orders.csv"],"order_id")
    require(sum(r["current_status"]=="OPEN" for r in initial_orders.values())==opened,"Initial open count differs")
    initial_delivered={d["order_id"] for d in initial["deliveries.csv"]}
    require(initial_delivered=={k for k,r in initial_orders.items() if r["current_status"]=="DELIVERED"},"Initial outcomes mismatch")
    late=0
    for key,row in orders.items():
        require(row["customer_id"] in current["customers.csv"],"Order references unknown customer")
        accepted,promised=parse_stamp(row["accepted_at"]),parse_stamp(row["promised_delivery_at_acceptance"])
        require(parse_stamp(row["source_created_at"])<=accepted<=parse_stamp(row["source_updated_at"]),"Invalid acceptance chronology")
        require(accepted<promised,"Promise must follow acceptance")
        require((key in deliveries)==(row["current_status"]=="DELIVERED"),"Open orders must not have completed outcomes")
        if key in deliveries:
            d=deliveries[key]
            dispatched,delivered=parse_stamp(d["dispatched_at"]),parse_stamp(d["delivered_at"])
            require(accepted<=dispatched<=delivered<=parse_stamp(d["source_updated_at"]),"Delivery chronology mismatch")
            require(delivered<=parse_stamp(row["source_updated_at"]),"Order status precedes delivery")
            late+=delivered>promised
    event_types={}
    for e in current["events.jsonl"].values():
        row=orders.get(e["order_id"])
        require(row is not None,"Event references unknown order")
        at=parse_stamp(e["event_at"])
        require(parse_stamp(row["accepted_at"])<=at<=parse_stamp(e["source_updated_at"]),"Event chronology mismatch")
        event_types.setdefault(e["order_id"],set()).add(e["event_type"])
        if e["event_type"]=="ACCEPTED": require(e["event_at"]==row["accepted_at"],"Acceptance event mismatch")
        else:
            require(e["order_id"] in deliveries,"Outcome event on open order")
            d=deliveries[e["order_id"]]
            require(at<=parse_stamp(d["delivered_at"]),"Event occurs after delivery")
            if e["event_type"] in ("DISPATCHED","DELIVERED"):
                require(e["event_at"]==d[e["event_type"].lower()+"_at"],"Delivery event mismatch")
    for key in orders:
        required={"ACCEPTED","DISPATCHED","DELIVERED"} if key in deliveries else {"ACCEPTED"}
        require(required<=event_types.get(key,set()),"Lifecycle events missing")
    new_ids=set(changes["new_order_ids"])
    require(len(new_ids.intersection(deliveries))==expected["closed_new_orders"],"Incremental closed count differs")
    summary=json.loads((root/"dataset_manifest.json").read_text())
    require(summary["initial"]==manifests["initial"]["row_counts"] and summary["incremental"]==manifests["incremental"]["row_counts"],"Root counts differ")
    require(summary["acceptance_features"]==FEATURES,"Feature contract differs")
    for name,sha in summary["documents"].items(): require(digest(root/"documents"/name)==sha,"Policy checksum differs")
    # A mixed target within each priority cohort demonstrates overlapping outcomes.
    cohorts={}
    for key,d in deliveries.items():
        p=orders[key]["priority_at_acceptance"]
        cohorts.setdefault(p,[0,0])[parse_stamp(d["delivered_at"])>parse_stamp(orders[key]["promised_delivery_at_acceptance"])]+=1
    if hist>=1000: require(all(a>0 and b>0 for a,b in cohorts.values()),"Target lacks outcome overlap")
    return dict(status="PASS",initial_rows=manifests["initial"]["row_counts"],incremental_rows=manifests["incremental"]["row_counts"],
                merged_orders=len(orders),completed_orders=len(deliveries),open_orders=len(orders)-len(deliveries),
                late_deliveries=late,late_rate=round(late/len(deliveries),4),priority_outcomes=cohorts,
                incremental_exact_duplicates=60,incremental_quarantined_orders=15)
