# Fabric notebook: Generate Synthetic Event Streams → Eventhouse (Direct Ingest)
#
# This notebook reads from the lh_trucking Lakehouse tables and generates
# realistic event streams, ingesting them directly into Eventhouse KQL
# tables using the Kusto Python SDK.
#
# Event types generated:
#   1. TelemetryEvent       — GPS/speed/fuel every 30s per active truck
#   2. EngineFaultEvent     — J1939 diagnostic trouble codes
#   3. GeofenceEvent        — Terminal arrival/departure
#   4. HOSStatusChangeEvent — Driver duty status transitions
#   5. LoadStatusEvent      — Load lifecycle status changes
#
# Demo scenarios injected:
#   - Late Arrival: truck falling behind schedule
#   - Breakdown: critical fault code during trip
#   - HOS Violation Risk: driver approaching limits
#   - Maintenance Due: odometer crossing threshold
#   - Load Reassignment: trip becomes infeasible
#
# Prerequisites:
#   - Attach to "lh_trucking" Lakehouse
#   - Run 01_load_reference_data notebook first
#   - Create Eventhouse tables using schemas/eventhouse_setup.kql
#   - Set KUSTO_URI and KUSTO_DATABASE below
#
# To run: Execute all cells in order.

# %% [markdown]
# ## Install Dependencies

# %%
%pip install azure-kusto-data azure-kusto-ingest azure-identity --quiet

# %% [markdown]
# ## Configuration

# %%
import json
import uuid
import random
import time
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2, degrees

# -----------------------------------------------------------------------
# Eventhouse connection
#
# KUSTO_URI: Your Eventhouse Query URI. Find it in:
#   Fabric workspace → click your Eventhouse → copy the "Query URI"
#   It looks like: https://<id>.kusto.fabric.microsoft.com
#
# KUSTO_DATABASE: The name of your KQL database inside the Eventhouse
# -----------------------------------------------------------------------
KUSTO_URI = "<YOUR_EVENTHOUSE_QUERY_URI>"
KUSTO_DATABASE = "<YOUR_KQL_DATABASE_NAME>"

# Simulation parameters
SIMULATION_DURATION_MINUTES = 10     # How long to run the simulation
TELEMETRY_INTERVAL_SECONDS = 30      # GPS update frequency
SCENARIO_INJECTION_ENABLED = True    # Inject demo scenarios
BATCH_SIZE = 100                     # Rows per ingest batch (Kusto streaming)
PRINT_EVENTS = True                  # Print events to console

random.seed(None)  # Use current time for varied runs

# %% [markdown]
# ## Connect to Eventhouse (Kusto)

# %%
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, DataFormat
from azure.identity import DefaultAzureCredential
import io
import pandas as pd

# In Fabric notebooks, DefaultAzureCredential automatically uses
# the notebook's identity (no secrets needed)
credential = DefaultAzureCredential()

kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
    KUSTO_URI, credential
)

kusto_client = KustoClient(kcsb)

# Ingest URI: replace the hostname prefix with "ingest-"
ingest_uri = KUSTO_URI.replace("https://", "https://ingest-")
ingest_kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
    ingest_uri, credential
)
ingest_client = QueuedIngestClient(ingest_kcsb)

# Verify connection
result = kusto_client.execute(KUSTO_DATABASE, ".show database schema | count")
for row in result.primary_results[0]:
    print(f"✓ Connected to Eventhouse: {KUSTO_URI}")
    print(f"  Database: {KUSTO_DATABASE}")

# %% [markdown]
# ## Ingestion Helper

# %%

# Column order for each table (must match the .create-merge-table schema)
TABLE_COLUMNS = {
    "TelemetryEvent": [
        "event_id", "event_type", "timestamp", "source",
        "truck_id", "trip_id", "driver_id",
        "latitude", "longitude", "speed_mph", "heading_degrees",
        "fuel_pct", "engine_temp_f", "oil_pressure_psi",
        "odometer_miles", "engine_rpm", "ambient_temp_f", "def_level_pct",
    ],
    "EngineFaultEvent": [
        "event_id", "event_type", "timestamp", "source",
        "truck_id", "trip_id", "driver_id",
        "spn", "fmi", "fault_description", "severity",
        "occurrence_count", "latitude", "longitude", "action",
    ],
    "GeofenceEvent": [
        "event_id", "event_type", "timestamp", "source",
        "truck_id", "trip_id", "driver_id",
        "terminal_id", "terminal_name", "geofence_event",
        "latitude", "longitude",
    ],
    "HOSStatusChangeEvent": [
        "event_id", "event_type", "timestamp", "source",
        "driver_id", "trip_id", "truck_id",
        "previous_status", "new_status",
        "driving_hours_used", "driving_hours_remaining",
        "duty_hours_used", "duty_hours_remaining",
        "cycle_hours_used", "cycle_hours_remaining",
        "break_time_remaining_minutes",
        "latitude", "longitude",
    ],
    "LoadStatusEvent": [
        "event_id", "event_type", "timestamp", "source",
        "load_id", "trip_id", "customer_id", "load_number",
        "previous_status", "new_status", "terminal_id",
        "latitude", "longitude", "estimated_arrival", "notes",
    ],
}

# Buffers: collect rows per table, flush when BATCH_SIZE is reached
_event_buffers = {table: [] for table in TABLE_COLUMNS}
_ingest_counts = {table: 0 for table in TABLE_COLUMNS}

def flatten_event(event):
    """Flatten the envelope + body into a single dict for tabular ingest."""
    row = {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "timestamp": event["timestamp"],
        "source": event["source"],
    }
    row.update(event["body"])
    return row

def buffer_event(event):
    """Add event to the appropriate table buffer; flush if full."""
    table = event["event_type"]
    row = flatten_event(event)
    _event_buffers[table].append(row)

    if len(_event_buffers[table]) >= BATCH_SIZE:
        flush_buffer(table)

def flush_buffer(table):
    """Ingest buffered rows into the Eventhouse table."""
    rows = _event_buffers[table]
    if not rows:
        return

    columns = TABLE_COLUMNS[table]
    df = pd.DataFrame(rows, columns=columns)

    props = IngestionProperties(
        database=KUSTO_DATABASE,
        table=table,
        data_format=DataFormat.CSV,
    )

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    csv_bytes = io.BytesIO(csv_buffer.getvalue().encode("utf-8"))

    ingest_client.ingest_from_stream(csv_bytes, ingestion_properties=props)
    _ingest_counts[table] += len(rows)
    _event_buffers[table] = []

def flush_all_buffers():
    """Flush all remaining buffered events."""
    for table in TABLE_COLUMNS:
        flush_buffer(table)

# %% [markdown]
# ## Load Reference Data from Lakehouse

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load all reference tables
terminals_df = spark.table("terminals").toPandas()
trucks_df = spark.table("trucks").toPandas()
trailers_df = spark.table("trailers").toPandas()
drivers_df = spark.table("drivers").toPandas()
customers_df = spark.table("customers").toPandas()
routes_df = spark.table("routes").toPandas()
loads_df = spark.table("loads").toPandas()
trips_df = spark.table("trips").toPandas()

# Build lookup dictionaries
terminals = {row["terminal_id"]: row.to_dict() for _, row in terminals_df.iterrows()}
trucks = {row["truck_id"]: row.to_dict() for _, row in trucks_df.iterrows()}
drivers = {row["driver_id"]: row.to_dict() for _, row in drivers_df.iterrows()}
routes = {row["route_id"]: row.to_dict() for _, row in routes_df.iterrows()}
customers = {row["customer_id"]: row.to_dict() for _, row in customers_df.iterrows()}
loads = {row["load_id"]: row.to_dict() for _, row in loads_df.iterrows()}

# Get active trips (in_progress)
active_trips = trips_df[trips_df["status"] == "in_progress"].to_dict("records")
print(f"Loaded {len(active_trips)} active trips for simulation")
print(f"Reference data: {len(terminals)} terminals, {len(trucks)} trucks, "
      f"{len(drivers)} drivers, {len(routes)} routes")

# %% [markdown]
# ## Helper Functions

# %%
def new_event_id():
    return str(uuid.uuid4())

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def bearing_degrees(lat1, lon1, lat2, lon2):
    """Calculate bearing from point 1 to point 2."""
    dlon = radians(lon2 - lon1)
    x = sin(dlon) * cos(radians(lat2))
    y = cos(radians(lat1)) * sin(radians(lat2)) - sin(radians(lat1)) * cos(radians(lat2)) * cos(dlon)
    return (degrees(atan2(x, y)) + 360) % 360

def interpolate_position(origin_lat, origin_lon, dest_lat, dest_lon, progress):
    """Linear interpolation with small random jitter for realism."""
    lat = origin_lat + (dest_lat - origin_lat) * progress + random.uniform(-0.02, 0.02)
    lon = origin_lon + (dest_lon - origin_lon) * progress + random.uniform(-0.02, 0.02)
    return round(lat, 4), round(lon, 4)

def make_envelope(event_type, source, body):
    """Wrap event body in standard envelope."""
    return {
        "event_id": new_event_id(),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": source,
        "body": body,
    }

# %% [markdown]
# ## J1939 Fault Code Reference

# %%
FAULT_CODES = [
    {"spn": 110, "fmi": 0, "description": "Engine Coolant Temperature - Above Normal Operating Range", "severity": "critical"},
    {"spn": 100, "fmi": 1, "description": "Engine Oil Pressure - Below Normal Operating Range", "severity": "critical"},
    {"spn": 111, "fmi": 0, "description": "Coolant Level - Above Normal Operating Range (Low Level)", "severity": "critical"},
    {"spn": 157, "fmi": 0, "description": "Fuel Rail Pressure - Above Normal Operating Range", "severity": "critical"},
    {"spn": 190, "fmi": 2, "description": "Engine Speed - Erratic/Intermittent", "severity": "warning"},
    {"spn": 94,  "fmi": 1, "description": "Fuel Delivery Pressure - Below Normal Operating Range", "severity": "warning"},
    {"spn": 91,  "fmi": 3, "description": "Throttle Position - Voltage Above Normal", "severity": "warning"},
    {"spn": 84,  "fmi": 2, "description": "Vehicle Speed Sensor - Erratic/Intermittent", "severity": "warning"},
    {"spn": 168, "fmi": 1, "description": "Battery Voltage - Below Normal Operating Range", "severity": "warning"},
    {"spn": 171, "fmi": 0, "description": "Ambient Air Temperature - Above Normal Operating Range", "severity": "info"},
]

# %% [markdown]
# ## Truck State Tracker

# %%
class TruckSimState:
    """Tracks the real-time simulated state of a truck on a trip."""

    def __init__(self, trip, route, origin_terminal, dest_terminal, truck, driver):
        self.trip = trip
        self.route = route
        self.origin = origin_terminal
        self.destination = dest_terminal
        self.truck = truck
        self.driver = driver

        # Position tracking
        self.progress = 0.0
        dep_str = trip.get("actual_departure") or trip["scheduled_departure"]
        try:
            self.departure_time = datetime.fromisoformat(dep_str.replace("Z", ""))
        except (ValueError, AttributeError):
            self.departure_time = datetime.utcnow() - timedelta(hours=3)

        total_hours = route["estimated_hours_with_stops"] or route["estimated_hours"]
        self.total_duration_hours = float(total_hours)
        elapsed = (datetime.utcnow() - self.departure_time).total_seconds() / 3600
        self.progress = min(max(elapsed / self.total_duration_hours, 0.01), 0.95)

        # Vehicle state
        self.speed_mph = random.uniform(55, 68)
        self.fuel_pct = random.uniform(40, 95)
        self.engine_temp_f = random.uniform(185, 205)
        self.oil_pressure_psi = random.uniform(35, 55)
        self.odometer = int(truck.get("odometer_miles", 300000))
        self.engine_rpm = random.randint(1200, 1600)
        self.def_level_pct = random.uniform(60, 95)

        # HOS state
        self.driving_hours_used = random.uniform(2, 8)
        self.duty_hours_used = self.driving_hours_used + random.uniform(0.5, 2)
        self.cycle_hours_used = random.uniform(30, 60)
        self.duty_status = "driving"
        self.break_minutes_remaining = max(0, 480 - self.driving_hours_used * 60)

        # Flags for scenario injection
        self.breakdown_injected = False
        self.late_arrival_injected = False
        self.hos_risk_injected = False
        self.maintenance_due_injected = False

    def advance(self, seconds):
        """Advance simulation state by given seconds."""
        hours = seconds / 3600
        distance_increment = self.speed_mph * hours

        total_miles = float(self.route["distance_miles"])
        self.progress = min(self.progress + (distance_increment / total_miles), 0.999)

        self.speed_mph = max(0, min(80, self.speed_mph + random.uniform(-3, 3)))
        self.fuel_pct = max(5, self.fuel_pct - random.uniform(0.01, 0.05))
        self.engine_temp_f = max(180, min(230, self.engine_temp_f + random.uniform(-2, 2)))
        self.oil_pressure_psi = max(25, min(65, self.oil_pressure_psi + random.uniform(-1, 1)))
        self.odometer += int(distance_increment)
        self.engine_rpm = max(600, min(2200, self.engine_rpm + random.randint(-50, 50)))
        self.def_level_pct = max(10, self.def_level_pct - random.uniform(0, 0.02))

        if self.duty_status == "driving":
            self.driving_hours_used += hours
            self.duty_hours_used += hours
            self.cycle_hours_used += hours
            self.break_minutes_remaining = max(0, self.break_minutes_remaining - (seconds / 60))

    @property
    def lat_lon(self):
        return interpolate_position(
            self.origin["latitude"], self.origin["longitude"],
            self.destination["latitude"], self.destination["longitude"],
            self.progress,
        )

    @property
    def heading(self):
        return int(bearing_degrees(
            self.origin["latitude"], self.origin["longitude"],
            self.destination["latitude"], self.destination["longitude"],
        ))

    @property
    def driving_hours_remaining(self):
        return max(0, 11.0 - self.driving_hours_used)

    @property
    def duty_hours_remaining(self):
        return max(0, 14.0 - self.duty_hours_used)

    @property
    def cycle_hours_remaining(self):
        return max(0, 70.0 - self.cycle_hours_used)

    @property
    def eta(self):
        remaining_miles = float(self.route["distance_miles"]) * (1 - self.progress)
        if self.speed_mph > 0:
            remaining_hours = remaining_miles / self.speed_mph
            return datetime.utcnow() + timedelta(hours=remaining_hours)
        return None

# %% [markdown]
# ## Event Generators

# %%
def generate_telemetry_event(state):
    """Generate a TelemetryEvent for a truck."""
    lat, lon = state.lat_lon
    return make_envelope("TelemetryEvent", "telematics-unit", {
        "truck_id": state.trip["truck_id"],
        "trip_id": state.trip["trip_id"],
        "driver_id": state.trip["driver_id"],
        "latitude": lat,
        "longitude": lon,
        "speed_mph": round(state.speed_mph, 1),
        "heading_degrees": state.heading,
        "fuel_pct": round(state.fuel_pct, 1),
        "engine_temp_f": round(state.engine_temp_f),
        "oil_pressure_psi": round(state.oil_pressure_psi),
        "odometer_miles": state.odometer,
        "engine_rpm": state.engine_rpm,
        "ambient_temp_f": random.randint(25, 85),
        "def_level_pct": round(state.def_level_pct, 1),
    })


def generate_engine_fault_event(state, fault=None):
    """Generate an EngineFaultEvent."""
    if fault is None:
        fault = random.choice(FAULT_CODES)
    lat, lon = state.lat_lon
    return make_envelope("EngineFaultEvent", "engine-ecu", {
        "truck_id": state.trip["truck_id"],
        "trip_id": state.trip["trip_id"],
        "driver_id": state.trip["driver_id"],
        "spn": fault["spn"],
        "fmi": fault["fmi"],
        "fault_description": fault["description"],
        "severity": fault["severity"],
        "occurrence_count": 1,
        "latitude": lat,
        "longitude": lon,
        "action": "set",
    })


def generate_geofence_event(state, terminal, event_type):
    """Generate a GeofenceEvent for terminal enter/exit."""
    lat, lon = state.lat_lon
    return make_envelope("GeofenceEvent", "geofence-service", {
        "truck_id": state.trip["truck_id"],
        "trip_id": state.trip["trip_id"],
        "driver_id": state.trip["driver_id"],
        "terminal_id": terminal["terminal_id"],
        "terminal_name": terminal["name"],
        "geofence_event": event_type,
        "latitude": lat,
        "longitude": lon,
    })


def generate_hos_status_change_event(state, new_status):
    """Generate an HOSStatusChangeEvent."""
    lat, lon = state.lat_lon
    prev_status = state.duty_status
    return make_envelope("HOSStatusChangeEvent", "eld-device", {
        "driver_id": state.trip["driver_id"],
        "trip_id": state.trip["trip_id"],
        "truck_id": state.trip["truck_id"],
        "previous_status": prev_status,
        "new_status": new_status,
        "driving_hours_used": round(state.driving_hours_used, 2),
        "driving_hours_remaining": round(state.driving_hours_remaining, 2),
        "duty_hours_used": round(state.duty_hours_used, 2),
        "duty_hours_remaining": round(state.duty_hours_remaining, 2),
        "cycle_hours_used": round(state.cycle_hours_used, 2),
        "cycle_hours_remaining": round(state.cycle_hours_remaining, 2),
        "break_time_remaining_minutes": round(state.break_minutes_remaining),
        "latitude": lat,
        "longitude": lon,
    })


def generate_load_status_event(state, new_status, terminal=None, notes=""):
    """Generate a LoadStatusEvent."""
    lat, lon = state.lat_lon
    load = loads.get(state.trip["load_id"], {})
    return make_envelope("LoadStatusEvent", "dispatch-system", {
        "load_id": state.trip["load_id"],
        "trip_id": state.trip["trip_id"],
        "customer_id": load.get("customer_id"),
        "load_number": load.get("load_number", "UNKNOWN"),
        "previous_status": load.get("status", "in_transit"),
        "new_status": new_status,
        "terminal_id": terminal["terminal_id"] if terminal else None,
        "latitude": lat,
        "longitude": lon,
        "estimated_arrival": state.eta.isoformat() + "Z" if state.eta else None,
        "notes": notes,
    })

# %% [markdown]
# ## Scenario Injectors

# %%
def check_and_inject_scenarios(state, events):
    """
    Check if any demo scenario conditions are met and inject events.
    Modifies state and appends to events list.
    """
    trip = state.trip

    # --- SCENARIO 1: Late Arrival ---
    if not state.late_arrival_injected and state.progress > 0.6:
        try:
            sched_arr = datetime.fromisoformat(trip["scheduled_arrival"].replace("Z", ""))
            if state.eta and state.eta > sched_arr + timedelta(hours=1):
                state.late_arrival_injected = True
                state.speed_mph = max(45, state.speed_mph - 10)
                load = loads.get(trip["load_id"], {})
                customer = customers.get(load.get("customer_id", ""), {})
                print(f"\n  🚨 SCENARIO: Late Arrival — Trip {trip['trip_number']}")
                print(f"     ETA: {state.eta.isoformat()}, Scheduled: {sched_arr.isoformat()}")
                print(f"     Customer: {customer.get('name', 'Unknown')} ({customer.get('contact_email', 'N/A')})")
                events.append(generate_load_status_event(
                    state, "in_transit", notes=f"ALERT: Late arrival expected. ETA {state.eta.isoformat()}Z"
                ))
        except (ValueError, TypeError):
            pass

    # --- SCENARIO 2: Breakdown ---
    if (not state.breakdown_injected
            and 0.3 < state.progress < 0.7
            and random.random() < 0.002):
        state.breakdown_injected = True
        critical_fault = random.choice([f for f in FAULT_CODES if f["severity"] == "critical"])
        state.speed_mph = 0
        state.engine_rpm = 0
        print(f"\n  🚨 SCENARIO: Breakdown — Trip {trip['trip_number']}")
        print(f"     Fault: SPN {critical_fault['spn']} / FMI {critical_fault['fmi']} — {critical_fault['description']}")
        events.append(generate_engine_fault_event(state, critical_fault))
        events.append(generate_load_status_event(
            state, "in_transit", notes=f"ALERT: Truck breakdown. {critical_fault['description']}. Service dispatch needed."
        ))

    # --- SCENARIO 3: HOS Violation Risk ---
    if (not state.hos_risk_injected
            and state.duty_status == "driving"
            and (state.driving_hours_remaining < 1.0 or state.duty_hours_remaining < 1.5)):
        state.hos_risk_injected = True
        driver = drivers.get(trip["driver_id"], {})
        print(f"\n  🚨 SCENARIO: HOS Violation Risk — Driver {driver.get('first_name', '')} {driver.get('last_name', '')}")
        print(f"     Driving remaining: {state.driving_hours_remaining:.1f}h, Duty remaining: {state.duty_hours_remaining:.1f}h")
        print(f"     Supervisor: {driver.get('supervisor_email', 'N/A')}")
        events.append(generate_hos_status_change_event(state, "driving"))

    # --- SCENARIO 4: Maintenance Due ---
    truck = trucks.get(trip["truck_id"], {})
    next_maint = truck.get("next_maintenance_miles", 999999999)
    if (not state.maintenance_due_injected
            and next_maint
            and state.odometer >= int(next_maint)):
        state.maintenance_due_injected = True
        print(f"\n  🚨 SCENARIO: Maintenance Due — Truck {truck.get('truck_number', 'Unknown')}")
        print(f"     Odometer: {state.odometer}, Threshold: {next_maint}")
        print(f"     Type: {truck.get('next_maintenance_type', 'Unknown')}")
        print(f"     Action: POST to maintenance scheduling API")

    # --- SCENARIO 5: Load Reassignment ---
    if state.breakdown_injected and not hasattr(state, '_reassignment_printed'):
        state._reassignment_printed = True
        load = loads.get(trip["load_id"], {})
        req_type = load.get("required_trailer_type", "dry_van")
        req_endorse = set(load.get("required_endorsements", []) or [])

        eligible_drivers = [
            d for d in drivers.values()
            if d["driver_id"] != trip["driver_id"]
            and d.get("status") == "available"
            and req_endorse.issubset(set(d.get("cdl_endorsements", []) or []))
        ]
        eligible_trucks_list = [
            t for t in trucks.values()
            if t["truck_id"] != trip["truck_id"]
            and t.get("status") == "available"
        ]

        print(f"\n  🚨 SCENARIO: Load Reassignment — Load {load.get('load_number', 'Unknown')}")
        print(f"     Required: trailer={req_type}, endorsements={req_endorse or 'none'}")
        print(f"     Eligible drivers: {len(eligible_drivers)}, Eligible trucks: {len(eligible_trucks_list)}")
        if eligible_drivers and eligible_trucks_list:
            alt_driver = random.choice(eligible_drivers)
            alt_truck = random.choice(eligible_trucks_list)
            print(f"     → Reassigning to Driver {alt_driver['first_name']} {alt_driver['last_name']} / Truck {alt_truck['truck_number']}")
        events.append(generate_load_status_event(
            state, "in_transit", notes="ALERT: Load reassignment in progress due to truck breakdown."
        ))

# %% [markdown]
# ## Initialize Simulation State

# %%
sim_states = []

for trip in active_trips:
    route = routes.get(trip["route_id"])
    if not route:
        continue

    origin_term = terminals.get(route["origin_terminal_id"])
    dest_term = terminals.get(route["destination_terminal_id"])
    truck = trucks.get(trip["truck_id"])
    driver = drivers.get(trip["driver_id"])

    if not all([origin_term, dest_term, truck, driver]):
        continue

    state = TruckSimState(trip, route, origin_term, dest_term, truck, driver)

    # Make some trucks late for demo scenario variety
    if random.random() < 0.15:
        state.speed_mph = random.uniform(35, 48)

    # Make some drivers have high HOS usage for demo
    if random.random() < 0.10:
        state.driving_hours_used = random.uniform(9.5, 10.5)
        state.duty_hours_used = state.driving_hours_used + random.uniform(1, 2.5)

    sim_states.append(state)

print(f"\nInitialized {len(sim_states)} truck simulations")
for s in sim_states[:5]:
    print(f"  Trip {s.trip['trip_number']}: {s.origin['city']} → {s.destination['city']}, "
          f"progress={s.progress:.0%}, speed={s.speed_mph:.0f}mph")
if len(sim_states) > 5:
    print(f"  ... and {len(sim_states) - 5} more")

# %% [markdown]
# ## Run Event Generation Loop

# %%
print(f"\n{'='*60}")
print(f"Starting event generation for {SIMULATION_DURATION_MINUTES} minutes")
print(f"Active trucks: {len(sim_states)}")
print(f"Telemetry interval: {TELEMETRY_INTERVAL_SECONDS}s")
print(f"Scenario injection: {'ON' if SCENARIO_INJECTION_ENABLED else 'OFF'}")
print(f"Destination: Eventhouse ({KUSTO_DATABASE})")
print(f"{'='*60}\n")

total_events = 0
start_time = time.time()
end_time = start_time + (SIMULATION_DURATION_MINUTES * 60)
tick = 0

try:
    while time.time() < end_time:
        tick += 1
        tick_events = []

        for state in sim_states:
            if state.speed_mph > 0 or not state.breakdown_injected:
                state.advance(TELEMETRY_INTERVAL_SECONDS)

            # 1. Telemetry event (every tick)
            tick_events.append(generate_telemetry_event(state))

            # 2. Check for geofence events (near destination)
            if state.progress > 0.95:
                tick_events.append(generate_geofence_event(state, state.destination, "enter"))
                tick_events.append(generate_load_status_event(
                    state, "delivered", state.destination, "Load delivered at destination terminal."
                ))

            # 3. Random engine fault events (low probability)
            if random.random() < 0.001:
                tick_events.append(generate_engine_fault_event(state))

            # 4. Inject demo scenarios
            if SCENARIO_INJECTION_ENABLED:
                check_and_inject_scenarios(state, tick_events)

        # Buffer all events for Kusto ingest
        for event in tick_events:
            if PRINT_EVENTS:
                print(f"  [{event['event_type']}] truck={event['body'].get('truck_id', 'N/A')[:8]}... "
                      f"lat={event['body'].get('latitude', 'N/A')}")
            buffer_event(event)

        total_events += len(tick_events)

        # Progress update every 10 ticks
        if tick % 10 == 0:
            flush_all_buffers()
            elapsed = time.time() - start_time
            print(f"\n--- Tick {tick} | {elapsed:.0f}s elapsed | {total_events} events generated ---")
            print(f"    Ingested: {dict(_ingest_counts)}")

        time.sleep(TELEMETRY_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\n\n⚠ Simulation stopped by user.")

finally:
    # Flush remaining events
    flush_all_buffers()
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Simulation complete")
    print(f"  Duration: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Total events generated: {total_events}")
    print(f"  Events ingested per table:")
    for table, count in _ingest_counts.items():
        print(f"    {table:<30} {count:>6}")
    print(f"{'='*60}")

# %% [markdown]
# ## Verify: Query Eventhouse Tables

# %%
print("Eventhouse row counts (may take a few minutes for queued ingestion to complete):\n")
for table in TABLE_COLUMNS:
    try:
        result = kusto_client.execute(KUSTO_DATABASE, f"{table} | count")
        for row in result.primary_results[0]:
            print(f"  {table:<30} {row[0]:>6} rows")
    except Exception as e:
        print(f"  {table:<30} ERROR: {e}")

# %%
# Sample: latest telemetry per truck
print("\nLatest telemetry per truck:")
query = """
TelemetryEvent
| summarize arg_max(timestamp, *) by truck_id
| project timestamp, truck_id, latitude, longitude, speed_mph, fuel_pct, odometer_miles
| order by timestamp desc
| take 10
"""
result = kusto_client.execute(KUSTO_DATABASE, query)
for row in result.primary_results[0]:
    print(f"  {row['timestamp']}  truck={str(row['truck_id'])[:8]}...  "
          f"({row['latitude']:.2f}, {row['longitude']:.2f})  "
          f"{row['speed_mph']:.0f}mph  fuel={row['fuel_pct']:.0f}%")

# %% [markdown]
# ## Event Summary
#
# Events are ingested directly into Eventhouse tables via the Kusto Python SDK.
# Queued ingestion may take 2-5 minutes to fully materialize.
#
# ### Scenario Trigger Summary:
# - **Late Arrival**: `LoadStatusEvent | where notes has "Late arrival"`
# - **Breakdown**: `EngineFaultEvent | where severity == "critical"`
# - **HOS Violation Risk**: `HOSStatusChangeEvent | where driving_hours_remaining < 1.0`
# - **Maintenance Due**: `TelemetryEvent | where odometer_miles > threshold` (join with trucks table)
# - **Load Reassignment**: `LoadStatusEvent | where notes has "Load reassignment"`
