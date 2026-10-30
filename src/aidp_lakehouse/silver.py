"""Deterministic, bounded initial-batch quality engine; no I/O or cloud writes.

Rules are derived from generator.py and the published source contract. Spark
adapts these typed records to managed Delta; no business rule lives in a notebook.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

from .aidp_bronze import ENTITIES, columns
from .generator import FEATURES

KEYS = dict(zip(ENTITIES, ("customer_id", "order_id", "delivery_id", "event_id")))
TIMESTAMPS = {"accepted_at", "promised_delivery_at_acceptance", "dispatched_at", "delivered_at",
              "event_at", "source_created_at", "source_updated_at"}
NUMERIC = {"row_version": "bigint", "line_count": "int", "units": "int", "transit_distance_km": "int",
           "inventory_fill_rate_at_acceptance": "decimal(6,3)", "warehouse_load_at_acceptance": "decimal(6,3)"}
RANGES = {"line_count": (1, 12), "units": (5, 400), "transit_distance_km": (50, 2400),
          "inventory_fill_rate_at_acceptance": (Decimal(".450"), Decimal("1.000")),
          "warehouse_load_at_acceptance": (Decimal(".350"), Decimal("1.300")),
          "row_version": (1, 2**63-1)}
REGIONS = {"NORTH", "SOUTH", "EAST", "WEST"}
SEGMENTS = {"STANDARD", "PREMIUM"}
PRIORITIES = {"NORMAL", "HIGH", "URGENT"}
CARRIERS = {"CARRIER_A", "CARRIER_B", "CARRIER_C"}
DOMAINS = {"region": REGIONS, "customer_region_at_acceptance": REGIONS,
           "segment": SEGMENTS, "customer_segment_at_acceptance": SEGMENTS,
           "priority_at_acceptance": PRIORITIES, "current_priority": PRIORITIES,
           "planned_carrier_at_acceptance": CARRIERS, "actual_carrier": CARRIERS,
           "current_status": {"OPEN", "DELIVERED"},
           "event_type": {"ACCEPTED", "DISPATCHED", "DELIVERED", "INCIDENT_OPENED"}}
CONTRACTS = {entity: [(name, "timestamp" if name in TIMESTAMPS else NUMERIC.get(name, dtype))
                      for name, dtype in columns(entity) if name != "_corrupt_record"] for entity in ENTITIES}
PROVENANCE = [("_batch_id", "string"), ("_source_file", "string"), ("_record_hash", "string")]
QUARANTINE_SCHEMA = [("entity", "string"), ("business_key", "string"), ("batch_id", "string"),
                     ("source_file", "string"), ("raw_record", "string"),
                     ("rejection_reasons", "array<string>"), ("occurrence_count", "bigint"),
                     ("rejection_id", "string")]


def canonical(value) -> str:
    def encode(item):
        if isinstance(item, datetime):
            return item.astimezone(timezone.utc).isoformat(timespec="microseconds")
        if isinstance(item, Decimal):
            return str(item)
        raise TypeError("Unsupported canonical value")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=encode)


def fingerprint(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def utc_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value):
        raise ValueError("Expected a full timezone-aware ISO timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass
class Record:
    entity: str
    raw: dict
    source: str
    data: dict = field(default_factory=dict)
    reasons: set = field(default_factory=set)


@dataclass
class SilverResult:
    tables: dict
    quarantine: list
    summary: dict
    business_fingerprints: dict


def _parse(entity, source, extracted):
    raw = {key: value for key, value in source.items() if key != "_source_file"}
    record = Record(entity, raw, source.get("_source_file", ""))
    if not isinstance(record.source, str) or not record.source.strip():
        record.reasons.add("MISSING_SOURCE_FILE")
    if raw.get("_corrupt_record"):
        record.reasons.add("PARSER_CORRUPT_RECORD")
    for name, dtype in CONTRACTS[entity]:
        value = raw.get(name)
        record.data[name] = None
        if value is None or isinstance(value, str) and not value.strip():
            record.reasons.add("MISSING_FIELD:" + name)
            continue
        try:
            if dtype == "timestamp":
                value = utc_timestamp(value)
            elif dtype in ("int", "bigint"):
                if isinstance(value, bool) or not re.fullmatch(r"[+-]?\d+", str(value)):
                    raise ValueError("Invalid integer")
                value = int(value)
                if not -(2**63) <= value < 2**63:
                    raise ValueError("Integer overflow")
            elif dtype.startswith("decimal"):
                value = Decimal(str(value))
                if not value.is_finite() or value.quantize(Decimal(".001")) != value:
                    raise ValueError("Non-finite or excess decimal scale")
                value = value.quantize(Decimal(".001"))
            elif dtype.startswith("map"):
                if not isinstance(value, dict) or not all(isinstance(k, str) and isinstance(v, str) for k,v in value.items()):
                    raise ValueError("Expected string map")
                value = dict(value)
            elif not isinstance(value, str):
                raise ValueError("Expected string")
        except (ValueError, TypeError, InvalidOperation, OverflowError):
            record.reasons.add("INVALID_CONVERSION:" + name)
            continue
        record.data[name] = value
        if name in RANGES and not RANGES[name][0] <= value <= RANGES[name][1]:
            record.reasons.add("OUT_OF_RANGE:" + name)
        if name in DOMAINS and value not in DOMAINS[name]:
            record.reasons.add("INVALID_DOMAIN:" + name)
    d = record.data

    def ordered(names, reason, strict=False):
        values = [d.get(name) for name in names]
        if all(isinstance(v, datetime) for v in values):
            if not all(a < b if strict else a <= b for a,b in zip(values, values[1:])):
                record.reasons.add(reason)

    ordered(["source_created_at", "source_updated_at"], "SOURCE_CHRONOLOGY")
    if d.get("source_updated_at") and d["source_updated_at"] > extracted:
        record.reasons.add("SOURCE_AFTER_EXTRACTION")
    if entity == "orders":
        ordered(["source_created_at", "accepted_at", "source_updated_at"], "ACCEPTANCE_CHRONOLOGY")
        ordered(["accepted_at", "promised_delivery_at_acceptance"], "PROMISE_NOT_AFTER_ACCEPTANCE", strict=True)
    elif entity == "deliveries":
        ordered(["source_created_at", "dispatched_at", "delivered_at", "source_updated_at"], "DELIVERY_CHRONOLOGY")
    elif entity == "events":
        ordered(["source_created_at", "event_at", "source_updated_at"], "EVENT_CHRONOLOGY")
        if d.get("event_type") == "INCIDENT_OPENED" and isinstance(d.get("payload"), dict):
            if d["payload"].get("exception_type") not in {"CAPACITY", "STOCK", "TRANSPORT"}:
                record.reasons.add("INVALID_INCIDENT_EXCEPTION_TYPE")
    return record


def _relationships(candidates):
    """Monotonic rejection to a consistent snapshot; cascades retain explicit reasons."""
    while True:
        before = sum(bool(r.reasons) for table in candidates.values() for r in table.values())
        good = {entity: {key:r for key,r in table.items() if not r.reasons} for entity,table in candidates.items()}
        deliveries, events = defaultdict(list), defaultdict(list)
        for r in good["deliveries"].values(): deliveries[r.data["order_id"]].append(r)
        for r in good["events"].values(): events[r.data["order_id"]].append(r)
        for key,r in good["orders"].items():
            d = r.data
            if d["customer_id"] not in good["customers"]: r.reasons.add("UNKNOWN_OR_REJECTED_CUSTOMER")
            ds = deliveries[key]
            if d["current_status"] == "OPEN" and ds: r.reasons.add("OPEN_ORDER_HAS_DELIVERY")
            if d["current_status"] == "DELIVERED" and len(ds) != 1: r.reasons.add("MISSING_OR_MULTIPLE_DELIVERIES")
            kinds = {e.data["event_type"] for e in events[key]}
            required = {"ACCEPTED"} | ({"DISPATCHED", "DELIVERED"} if d["current_status"] == "DELIVERED" else set())
            if not required <= kinds: r.reasons.add("MISSING_LIFECYCLE_EVENTS")
        for r in good["deliveries"].values():
            d = r.data; parent = good["orders"].get(d["order_id"])
            if not parent: r.reasons.add("UNKNOWN_OR_REJECTED_ORDER"); continue
            p = parent.data
            if len(deliveries[d["order_id"]]) != 1: r.reasons.add("MULTIPLE_DELIVERIES_FOR_ORDER")
            if p["current_status"] != "DELIVERED": r.reasons.add("DELIVERY_FOR_OPEN_ORDER")
            if not p["accepted_at"] <= d["dispatched_at"] <= d["delivered_at"] <= p["source_updated_at"]:
                r.reasons.add("DELIVERY_ORDER_CHRONOLOGY")
        for r in good["events"].values():
            d = r.data; parent = good["orders"].get(d["order_id"])
            if not parent: r.reasons.add("UNKNOWN_OR_REJECTED_ORDER"); continue
            p = parent.data
            if d["event_at"] < p["accepted_at"]: r.reasons.add("EVENT_BEFORE_ACCEPTANCE")
            if d["event_type"] == "ACCEPTED":
                if d["event_at"] != p["accepted_at"]: r.reasons.add("ACCEPTANCE_EVENT_MISMATCH")
            else:
                ds = deliveries[d["order_id"]]
                if len(ds) != 1: r.reasons.add("MISSING_DELIVERY_FOR_EVENT"); continue
                delivery = ds[0].data
                if d["event_at"] > delivery["delivered_at"]: r.reasons.add("EVENT_AFTER_DELIVERY")
                field_name = {"DISPATCHED":"dispatched_at", "DELIVERED":"delivered_at"}.get(d["event_type"])
                if field_name and d["event_at"] != delivery[field_name]: r.reasons.add("DELIVERY_EVENT_MISMATCH")
        after = sum(bool(r.reasons) for table in candidates.values() for r in table.values())
        if after == before: return


def transform_initial(raw_tables: dict, *, batch_id="initial", extracted_at="2026-10-28T00:00:00Z") -> SilverResult:
    if batch_id != "initial" or set(raw_tables) != set(ENTITIES):
        raise ValueError("Supply exactly the four initial Bronze entities")
    extracted = utc_timestamp(extracted_at)
    records, groups, candidates = {}, {}, {}
    for entity in ENTITIES:
        records[entity] = [_parse(entity, row, extracted) for row in raw_tables[entity]]
        groups[entity] = defaultdict(list); candidates[entity] = {}
        for r in records[entity]:
            if not r.reasons: groups[entity][r.data[KEYS[entity]]].append(r)
        for key, group in groups[entity].items():
            by_precedence = defaultdict(set)
            for r in group:
                by_precedence[(r.data["row_version"], r.data["source_updated_at"])].add(canonical(r.raw))
            conflict = any(len(values)>1 for values in by_precedence.values())
            immutable = ["source_created_at"] + (FEATURES + ["customer_id"] if entity == "orders" else [])
            immutable_change = len({canonical({name:r.data[name] for name in immutable}) for r in group}) > 1
            winner = max(group, key=lambda r:(r.data["row_version"], r.data["source_updated_at"],
                                              r.source, canonical(r.raw)))
            if conflict or immutable_change:
                for r in group:
                    if conflict: r.reasons.add("CONFLICTING_EQUAL_PRECEDENCE")
                    if immutable_change: r.reasons.add("IMMUTABLE_SOURCE_ATTRIBUTES_CHANGED")
            else: candidates[entity][key] = winner
    _relationships(candidates)
    tables, summary, quarantine = {}, {}, {}
    for entity in ENTITIES:
        tables[entity] = []
        for key, group in groups[entity].items():
            chosen = candidates[entity].get(key)
            if chosen and chosen.reasons:
                for r in group: r.reasons.update(chosen.reasons)
            if chosen and not chosen.reasons:
                tables[entity].append({**chosen.data, "_batch_id":batch_id, "_source_file":chosen.source,
                                       "_record_hash":fingerprint(chosen.data)})
        rejected = 0
        for r in records[entity]:
            if not r.reasons: continue
            rejected += 1
            raw_record = canonical(r.raw)
            rid = fingerprint([entity, batch_id, r.source, raw_record])
            if rid not in quarantine:
                raw_key = r.raw.get(KEYS[entity])
                quarantine[rid] = dict(entity=entity, business_key=None if raw_key is None else str(raw_key),
                                       batch_id=batch_id, source_file=r.source, raw_record=raw_record,
                                       rejection_reasons=sorted(r.reasons), occurrence_count=0, rejection_id=rid)
            if quarantine[rid]["rejection_reasons"] != sorted(r.reasons):
                raise AssertionError("Identical source records acquired inconsistent dispositions")
            quarantine[rid]["occurrence_count"] += 1
        tables[entity].sort(key=lambda row:row[KEYS[entity]])
        accepted = len(tables[entity]); raw_count = len(records[entity])
        summary[entity] = dict(raw_rows=raw_count, duplicate_removals=raw_count-rejected-accepted,
                               rejected_rows=rejected, accepted_rows=accepted)
        assert summary[entity]["duplicate_removals"] >= 0
        assert raw_count == sum(summary[entity][key] for key in ("duplicate_removals", "rejected_rows", "accepted_rows"))
    fingerprints = {entity:fingerprint([(r[KEYS[entity]],r["_record_hash"]) for r in rows]) for entity,rows in tables.items()}
    return SilverResult(tables, sorted(quarantine.values(), key=lambda r:r["rejection_id"]), summary, fingerprints)
