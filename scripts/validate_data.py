"""Validate referential integrity of generated JSONL reference data."""
import json, os, sys

data_dir = os.path.join(os.path.dirname(__file__), "..", "reference_data")
files = {
    "terminals": "terminal_id",
    "trucks": "truck_id",
    "trailers": "trailer_id",
    "drivers": "driver_id",
    "customers": "customer_id",
    "routes": "route_id",
    "loads": "load_id",
    "trips": "trip_id",
    "maintenance_events": "maintenance_event_id",
    "service_tickets": "service_ticket_id",
    "driver_hos_logs": "hos_log_id",
}

all_data = {}
all_ids = {}
errors = []

print("Record counts & PK uniqueness:")
for name, pk in files.items():
    path = os.path.join(data_dir, f"{name}.jsonl")
    records = []
    with open(path, "r") as f:
        for line_num, line in enumerate(f, 1):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                errors.append(f"{name}.jsonl line {line_num}: Invalid JSON: {e}")
    all_data[name] = records
    ids = [r[pk] for r in records]
    all_ids[name] = set(ids)
    unique = len(ids) == len(set(ids))
    if not unique:
        errors.append(f"{name}: DUPLICATE primary keys detected!")
    print(f"  {name:<25} {len(records):>4} records, PK unique: {unique}")

print("\nForeign key validation:")
fk_checks = [
    ("trucks", "home_terminal_id", "terminals"),
    ("trailers", "home_terminal_id", "terminals"),
    ("drivers", "home_terminal_id", "terminals"),
    ("routes", "origin_terminal_id", "terminals"),
    ("routes", "destination_terminal_id", "terminals"),
    ("loads", "customer_id", "customers"),
    ("loads", "pickup_terminal_id", "terminals"),
    ("loads", "delivery_terminal_id", "terminals"),
    ("trips", "driver_id", "drivers"),
    ("trips", "truck_id", "trucks"),
    ("trips", "trailer_id", "trailers"),
    ("trips", "load_id", "loads"),
    ("trips", "route_id", "routes"),
    ("maintenance_events", "truck_id", "trucks"),
    ("maintenance_events", "terminal_id", "terminals"),
    ("service_tickets", "truck_id", "trucks"),
    ("driver_hos_logs", "driver_id", "drivers"),
]

for table, fk_col, ref_table in fk_checks:
    ref_ids = all_ids[ref_table]
    orphans = sum(1 for rec in all_data[table] if rec.get(fk_col) and rec[fk_col] not in ref_ids)
    status = "OK" if orphans == 0 else f"FAIL ({orphans} orphans)"
    print(f"  {table}.{fk_col} -> {ref_table}: {status}")
    if orphans > 0:
        errors.append(f"{table}.{fk_col} has {orphans} orphan references to {ref_table}")

print("\nRoute distance check (sample):")
for route in all_data["routes"][:5]:
    print(f"  {route['route_name']}: {route['distance_miles']} mi, "
          f"{route['estimated_hours']}h drive, {route['estimated_hours_with_stops']}h w/stops")

print("\nTrip status distribution:")
statuses = {}
for t in all_data["trips"]:
    statuses[t["status"]] = statuses.get(t["status"], 0) + 1
for s, c in sorted(statuses.items()):
    print(f"  {s}: {c}")

print()
if errors:
    print(f"ERRORS FOUND: {len(errors)}")
    for e in errors:
        print(f"  ! {e}")
    sys.exit(1)
else:
    print("ALL VALIDATION PASSED")
