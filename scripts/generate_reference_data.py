"""
Generate synthetic reference data for the Long-Haul Trucking Ontology demo.
Produces 11 JSONL files in the ../reference_data/ folder.

Usage:
    cd scripts
    python generate_reference_data.py
"""

import json
import uuid
import random
import os
from datetime import datetime, timedelta, date
from math import radians, sin, cos, sqrt, atan2

random.seed(42)

OUTPUT_DIR = os.environ.get(
    "REFERENCE_OUTPUT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "reference_data"),
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_id():
    return str(uuid.uuid4())

def write_jsonl(filename, records):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")
    # size_kb = os.path.getsize(path) / 1024
    # print(f"  wrote {filename:<40} {len(records):>5} records  {size_kb:>7.1f} KB")
    print(f"  Wrote {len(records):>4} records → {filename}")

def haversine_miles(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in miles."""
    R = 3958.8  # Earth radius in miles
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

def random_date(start, end):
    """Random date between start and end (date objects)."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def random_datetime(start, end):
    """Random datetime between start and end (datetime objects)."""
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.randint(0, int(delta)))

def random_phone():
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"

def random_vin(make_code):
    """Generate a plausible 17-char VIN."""
    chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    return make_code + "".join(random.choices(chars, k=17-len(make_code)))

# ---------------------------------------------------------------------------
# 1. Terminals – 15 real US trucking hubs
# ---------------------------------------------------------------------------

TERMINAL_DATA = [
    ("Atlanta Hub",         "Atlanta",        "GA", 33.7490, -84.3880, "America/New_York"),
    ("Chicago Hub",         "Chicago",        "IL", 41.8781, -87.6298, "America/Chicago"),
    ("Dallas Hub",          "Dallas",         "TX", 32.7767, -96.7970, "America/Chicago"),
    ("Denver Hub",          "Denver",         "CO", 39.7392, -104.9903, "America/Denver"),
    ("Houston Hub",         "Houston",        "TX", 29.7604, -95.3698, "America/Chicago"),
    ("Indianapolis Hub",    "Indianapolis",   "IN", 39.7684, -86.1581, "America/Indiana/Indianapolis"),
    ("Jacksonville Hub",    "Jacksonville",   "FL", 30.3322, -81.6557, "America/New_York"),
    ("Kansas City Hub",     "Kansas City",    "MO", 39.0997, -94.5786, "America/Chicago"),
    ("Los Angeles Hub",     "Los Angeles",    "CA", 33.9425, -118.2551, "America/Los_Angeles"),
    ("Memphis Hub",         "Memphis",        "TN", 35.1495, -90.0490, "America/Chicago"),
    ("Nashville Hub",       "Nashville",      "TN", 36.1627, -86.7816, "America/Chicago"),
    ("Newark Hub",          "Newark",         "NJ", 40.7357, -74.1724, "America/New_York"),
    ("Phoenix Hub",         "Phoenix",        "AZ", 33.4484, -112.0740, "America/Phoenix"),
    ("Salt Lake City Hub",  "Salt Lake City", "UT", 40.7608, -111.8910, "America/Denver"),
    ("Seattle Hub",         "Seattle",        "WA", 47.6062, -122.3321, "America/Los_Angeles"),
]

def generate_terminals():
    terminals = []
    for name, city, state, lat, lon, tz in TERMINAL_DATA:
        terminals.append({
            "terminal_id": new_id(),
            "name": name,
            "city": city,
            "state": state,
            "latitude": lat,
            "longitude": lon,
            "timezone": tz,
            "capacity_trucks": random.choice([40, 50, 60, 75, 80, 100]),
            "has_maintenance_bay": random.random() < 0.75,
            "address": f"{random.randint(100,9999)} Industrial Blvd",
        })
    return terminals

# ---------------------------------------------------------------------------
# 2. Trucks – 50 tractor units
# ---------------------------------------------------------------------------

TRUCK_MAKES = [
    ("Freightliner", "Cascadia", "1FU"),
    ("Kenworth",     "T680",     "1XK"),
    ("Peterbilt",    "579",      "1XP"),
    ("Volvo",        "VNL 860",  "4V4"),
    ("International","LT",       "3HS"),
    ("Mack",         "Anthem",   "1M1"),
]

MAINTENANCE_TYPES = [
    ("oil_change",           25000),
    ("tire_replacement",     50000),
    ("brake_inspection",     30000),
    ("dpf_cleaning",        200000),
    ("transmission_service", 100000),
    ("dot_inspection",       None),   # annual, not mileage-based
]

def generate_trucks(terminals):
    trucks = []
    for i in range(50):
        make, model, vin_prefix = random.choice(TRUCK_MAKES)
        year = random.randint(2019, 2025)
        odometer = random.randint(80000, 650000)

        # Calculate next maintenance
        maint_type, maint_interval = random.choice(MAINTENANCE_TYPES[:5])  # skip DOT for mileage calc
        next_maint_miles = ((odometer // maint_interval) + 1) * maint_interval

        dot_base = date(2025, 1, 1)
        last_dot = random_date(dot_base, date(2025, 12, 31))

        trucks.append({
            "truck_id": new_id(),
            "truck_number": f"TRK-{1001 + i}",
            "vin": random_vin(vin_prefix),
            "make": make,
            "model": model,
            "year": year,
            "odometer_miles": odometer,
            "fuel_capacity_gallons": random.choice([150, 200, 250, 300]),
            "status": "available",  # will be updated when trips are assigned
            "home_terminal_id": random.choice(terminals)["terminal_id"],
            "next_maintenance_miles": next_maint_miles,
            "next_maintenance_type": maint_type,
            "last_dot_inspection_date": str(last_dot),
        })
    return trucks

# ---------------------------------------------------------------------------
# 3. Trailers – 60 units
# ---------------------------------------------------------------------------

TRAILER_TYPES = [
    ("dry_van",  53, 45000),
    ("dry_van",  53, 45000),
    ("dry_van",  48, 42000),
    ("reefer",   53, 43000),
    ("reefer",   48, 40000),
    ("flatbed",  53, 48000),
    ("tanker",   42, 50000),
]

def generate_trailers(terminals):
    trailers = []
    for i in range(60):
        ttype, length, max_weight = random.choice(TRAILER_TYPES)
        trailers.append({
            "trailer_id": new_id(),
            "trailer_number": f"TRL-{2001 + i}",
            "type": ttype,
            "length_ft": length,
            "max_weight_lbs": max_weight,
            "status": "available",
            "home_terminal_id": random.choice(terminals)["terminal_id"],
            "year": random.randint(2017, 2025),
            "last_inspection_date": str(random_date(date(2025, 6, 1), date(2026, 1, 31))),
        })
    return trailers

# ---------------------------------------------------------------------------
# 4. Drivers – 65 CDL holders
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "James","Robert","John","Michael","David","William","Richard","Joseph","Thomas","Christopher",
    "Daniel","Matthew","Anthony","Mark","Steven","Paul","Andrew","Joshua","Kenneth","Kevin",
    "Maria","Jennifer","Linda","Patricia","Elizabeth","Susan","Jessica","Sarah","Karen","Lisa",
    "Nancy","Betty","Margaret","Sandra","Ashley","Dorothy","Kimberly","Emily","Donna","Michelle",
    "Carlos","Miguel","Juan","Luis","Jorge","Pedro","Rafael","Diego","Antonio","Fernando",
    "Aisha","Fatima","Priya","Wei","Yuki","Olga","Svetlana","Ingrid","Amara","Kenji",
    "Marcus","Tyrone","Deshawn","Jamal","Terrance",
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
    "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
    "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
    "Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts",
    "Patel","Kim","Singh","Chen","O'Brien","Murphy","Sullivan","Cohen","Yamamoto","Petrov",
    "Okafor","Washington","Freeman","Brooks","Howard",
]

ENDORSEMENT_COMBOS = [
    [],          # no special endorsements (most common)
    [],
    [],
    ["N"],       # tanker
    ["N"],
    ["H"],       # hazmat
    ["H", "N"],  # hazmat + tanker = X endorsement effectively
    ["T"],       # doubles/triples
    ["H"],
    ["N"],
]

CDL_STATES = ["GA","IL","TX","CO","IN","FL","MO","CA","TN","NJ","AZ","UT","WA","OH","PA"]

def generate_drivers(terminals):
    drivers = []
    used_names = set()
    for i in range(65):
        while True:
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            if (first, last) not in used_names:
                used_names.add((first, last))
                break

        cdl_state = random.choice(CDL_STATES)
        endorsements = random.choice(ENDORSEMENT_COMBOS)
        terminal = random.choice(terminals)
        hire_date = random_date(date(2015, 1, 1), date(2025, 6, 1))

        drivers.append({
            "driver_id": new_id(),
            "employee_id": f"DRV-{5001 + i}",
            "first_name": first,
            "last_name": last,
            "cdl_number": f"{cdl_state}{random.randint(10000000, 99999999)}",
            "cdl_state": cdl_state,
            "cdl_endorsements": endorsements,
            "cdl_expiration_date": str(random_date(date(2026, 6, 1), date(2029, 12, 31))),
            "hire_date": str(hire_date),
            "status": "available",
            "home_terminal_id": terminal["terminal_id"],
            "phone": random_phone(),
            "email": f"{first.lower()}.{last.lower()}@acmetrucking.com",
            "supervisor_email": f"supervisor.{terminal['city'].lower().replace(' ','')}@acmetrucking.com",
        })
    return drivers

# ---------------------------------------------------------------------------
# 5. Customers – 20 fictional companies
# ---------------------------------------------------------------------------

CUSTOMER_DATA = [
    ("Heartland Foods Inc.",         "Manufacturing", "Des Moines",    "IA"),
    ("Pacific Coast Electronics",    "Electronics",   "San Jose",      "CA"),
    ("Southern Timber Co.",          "Lumber",        "Savannah",      "GA"),
    ("Great Plains Agriculture",     "Agriculture",   "Omaha",         "NE"),
    ("Northeast Pharma Supply",      "Pharmaceutical","Boston",        "MA"),
    ("Midwest Auto Parts",           "Automotive",    "Detroit",       "MI"),
    ("Sunshine Beverages",           "Beverages",     "Orlando",       "FL"),
    ("Rocky Mountain Mining",        "Mining",        "Billings",      "MT"),
    ("Gulf Petrochemicals",          "Chemical",      "Beaumont",      "TX"),
    ("Liberty Steel Works",          "Steel",         "Pittsburgh",    "PA"),
    ("Cascade Paper Products",       "Paper",         "Portland",      "OR"),
    ("Prairie Grain Cooperative",    "Agriculture",   "Wichita",       "KS"),
    ("Atlantic Seafood Distributors","Food",          "Baltimore",     "MD"),
    ("Desert Solar Components",      "Energy",        "Tucson",        "AZ"),
    ("Appalachian Furniture Co.",    "Furniture",     "Asheville",     "NC"),
    ("Columbia River Produce",       "Agriculture",   "Yakima",        "WA"),
    ("Bayou Chemical Supply",        "Chemical",      "Baton Rouge",   "LA"),
    ("Lakeshore Retail Logistics",   "Retail",        "Milwaukee",     "WI"),
    ("Canyon Concrete & Aggregate",  "Construction",  "Flagstaff",     "AZ"),
    ("Tidewater Frozen Foods",       "Food",          "Norfolk",       "VA"),
]

def generate_customers():
    customers = []
    for name, industry, city, state in CUSTOMER_DATA:
        first = random.choice(FIRST_NAMES[:20])
        last = random.choice(LAST_NAMES[:20])
        customers.append({
            "customer_id": new_id(),
            "name": name,
            "contact_name": f"{first} {last}",
            "contact_email": f"{first.lower()}.{last.lower()}@{name.split()[0].lower()}.com",
            "contact_phone": random_phone(),
            "address": f"{random.randint(100,9999)} Commerce Dr",
            "city": city,
            "state": state,
            "industry": industry,
        })
    return customers

# ---------------------------------------------------------------------------
# 6. Routes – 40 realistic lanes between terminals
# ---------------------------------------------------------------------------

# Define routes as (origin_index, dest_index) referencing TERMINAL_DATA
ROUTE_PAIRS = [
    (0, 1),   # Atlanta → Chicago
    (1, 0),   # Chicago → Atlanta
    (0, 7),   # Atlanta → Kansas City
    (0, 10),  # Atlanta → Nashville
    (0, 6),   # Atlanta → Jacksonville
    (1, 5),   # Chicago → Indianapolis
    (1, 9),   # Chicago → Memphis
    (1, 11),  # Chicago → Newark
    (1, 7),   # Chicago → Kansas City
    (2, 4),   # Dallas → Houston
    (4, 2),   # Houston → Dallas
    (2, 9),   # Dallas → Memphis
    (2, 3),   # Dallas → Denver
    (2, 12),  # Dallas → Phoenix
    (3, 13),  # Denver → Salt Lake City
    (3, 7),   # Denver → Kansas City
    (3, 12),  # Denver → Phoenix
    (4, 6),   # Houston → Jacksonville
    (5, 10),  # Indianapolis → Nashville
    (5, 1),   # Indianapolis → Chicago
    (6, 0),   # Jacksonville → Atlanta
    (7, 3),   # Kansas City → Denver
    (7, 2),   # Kansas City → Dallas
    (7, 9),   # Kansas City → Memphis
    (8, 12),  # Los Angeles → Phoenix
    (8, 14),  # Los Angeles → Seattle
    (8, 13),  # Los Angeles → Salt Lake City
    (9, 0),   # Memphis → Atlanta
    (9, 2),   # Memphis → Dallas
    (9, 10),  # Memphis → Nashville
    (10, 0),  # Nashville → Atlanta
    (10, 9),  # Nashville → Memphis
    (11, 1),  # Newark → Chicago
    (11, 6),  # Newark → Jacksonville
    (12, 8),  # Phoenix → Los Angeles
    (12, 2),  # Phoenix → Dallas
    (13, 3),  # Salt Lake City → Denver
    (13, 14), # Salt Lake City → Seattle
    (14, 8),  # Seattle → Los Angeles
    (14, 13), # Seattle → Salt Lake City
]

def generate_routes(terminals):
    routes = []
    for orig_idx, dest_idx in ROUTE_PAIRS:
        orig = terminals[orig_idx]
        dest = terminals[dest_idx]
        dist = haversine_miles(orig["latitude"], orig["longitude"],
                               dest["latitude"], dest["longitude"])
        # Road distance is roughly 1.2-1.35x great circle for US interstates
        road_factor = random.uniform(1.20, 1.35)
        road_miles = round(dist * road_factor, 1)
        # Average speed ~55 mph for trucks (including traffic, terrain)
        est_hours = round(road_miles / 55.0, 1)
        # Add ~1.5 hours per 500 miles for fuel/rest stops
        est_hours_stops = round(est_hours + (road_miles / 500.0) * 1.5, 1)
        fuel_stops = max(1, int(road_miles / 450))

        routes.append({
            "route_id": new_id(),
            "route_name": f"{orig['city'][:3].upper()}→{dest['city'][:3].upper()}",
            "origin_terminal_id": orig["terminal_id"],
            "destination_terminal_id": dest["terminal_id"],
            "distance_miles": road_miles,
            "estimated_hours": est_hours,
            "estimated_hours_with_stops": est_hours_stops,
            "waypoints": json.dumps([]),  # simplified for demo
            "toll_cost_estimate": round(random.uniform(15, 120), 2),
            "fuel_stops_recommended": fuel_stops,
        })
    return routes

# ---------------------------------------------------------------------------
# 7. Loads – 30 freight loads
# ---------------------------------------------------------------------------

LOAD_TYPES = [
    ("general",       "dry_van",  []),
    ("general",       "dry_van",  []),
    ("general",       "dry_van",  []),
    ("general",       "flatbed",  []),
    ("refrigerated",  "reefer",   []),
    ("refrigerated",  "reefer",   []),
    ("hazmat",        "tanker",   ["H", "N"]),
    ("hazmat",        "dry_van",  ["H"]),
    ("oversize",      "flatbed",  []),
]

CARGO_DESCRIPTIONS = {
    "general":      ["Retail merchandise", "Auto parts", "Furniture", "Paper products",
                     "Building materials", "Consumer electronics", "Canned goods",
                     "Appliances", "Textiles", "Industrial equipment"],
    "refrigerated": ["Frozen produce", "Dairy products", "Fresh seafood",
                     "Pharmaceutical supplies (cold chain)", "Frozen meals"],
    "hazmat":       ["Diesel fuel", "Industrial chemicals", "Compressed gas cylinders",
                     "Fertilizer (ammonium nitrate)", "Paint and solvents"],
    "oversize":     ["Wind turbine blade", "Steel I-beams", "Heavy machinery",
                     "Prefabricated structure", "Mining equipment"],
}

def generate_loads(customers, terminals, routes):
    loads = []
    now = datetime(2026, 3, 17, 12, 0, 0)

    for i in range(30):
        load_type, trailer_type, endorsements = random.choice(LOAD_TYPES)
        customer = random.choice(customers)
        route = random.choice(routes)

        # Find matching terminal IDs from route
        pickup_tid = route["origin_terminal_id"]
        delivery_tid = route["destination_terminal_id"]

        # Pickup within next 0-3 days, delivery based on route time
        pickup_start = now + timedelta(hours=random.randint(-48, 72))
        pickup_end = pickup_start + timedelta(hours=random.randint(2, 6))
        drive_hours = route["estimated_hours_with_stops"]
        delivery_start = pickup_start + timedelta(hours=drive_hours - 2)
        delivery_end = pickup_start + timedelta(hours=drive_hours + 4)

        weight = random.randint(8000, 44000)
        description = random.choice(CARGO_DESCRIPTIONS[load_type])

        loads.append({
            "load_id": new_id(),
            "load_number": f"LD-{90001 + i}",
            "customer_id": customer["customer_id"],
            "load_type": load_type,
            "description": description,
            "weight_lbs": weight,
            "required_trailer_type": trailer_type,
            "required_endorsements": endorsements,
            "pickup_terminal_id": pickup_tid,
            "delivery_terminal_id": delivery_tid,
            "pickup_window_start": pickup_start.isoformat() + "Z",
            "pickup_window_end": pickup_end.isoformat() + "Z",
            "delivery_window_start": delivery_start.isoformat() + "Z",
            "delivery_window_end": delivery_end.isoformat() + "Z",
            "status": "pending",  # will be updated for assigned/in_transit
            "priority": random.choice(["standard", "standard", "standard", "expedited", "critical"]),
            "value_usd": round(random.uniform(5000, 250000), 2),
        })
    return loads

# ---------------------------------------------------------------------------
# 8. Trips – 30 dispatch records (20 active, 10 completed)
# ---------------------------------------------------------------------------

def generate_trips(drivers, trucks, trailers, loads, routes, terminals):
    trips = []
    now = datetime(2026, 3, 17, 12, 0, 0)

    available_drivers = [d for d in drivers]
    available_trucks = [t for t in trucks]
    available_trailers_by_type = {}
    for t in trailers:
        available_trailers_by_type.setdefault(t["type"], []).append(t)

    random.shuffle(available_drivers)
    random.shuffle(available_trucks)

    for i in range(30):
        load = loads[i]
        route = [r for r in routes
                 if r["origin_terminal_id"] == load["pickup_terminal_id"]
                 and r["destination_terminal_id"] == load["delivery_terminal_id"]]

        if not route:
            # Find any route and override
            route = [random.choice(routes)]
            load["pickup_terminal_id"] = route[0]["origin_terminal_id"]
            load["delivery_terminal_id"] = route[0]["destination_terminal_id"]
        route = route[0]

        # Assign resources
        driver = available_drivers[i % len(available_drivers)]
        truck = available_trucks[i % len(available_trucks)]

        # Find matching trailer type
        req_type = load["required_trailer_type"]
        if req_type in available_trailers_by_type and available_trailers_by_type[req_type]:
            trailer = available_trailers_by_type[req_type][i % len(available_trailers_by_type[req_type])]
        else:
            trailer = random.choice(trailers)

        # Trip timing
        if i < 10:
            # Completed trips (past)
            status = "completed"
            dep = now - timedelta(hours=random.randint(48, 168))
            drive_h = route["estimated_hours_with_stops"]
            arr = dep + timedelta(hours=drive_h + random.uniform(-1, 2))
            actual_dep = dep + timedelta(minutes=random.randint(-30, 60))
            actual_arr = arr + timedelta(minutes=random.randint(-60, 120))
            odom_start = truck["odometer_miles"] - random.randint(200, 800)
            odom_end = odom_start + int(route["distance_miles"])
            cur_lat = None
            cur_lon = None
            load["status"] = "delivered"
            driver["status"] = "available"
            truck["status"] = "available"
        elif i < 28:
            # Active trips (in progress)
            status = "in_progress"
            dep = now - timedelta(hours=random.randint(2, 20))
            drive_h = route["estimated_hours_with_stops"]
            arr = dep + timedelta(hours=drive_h)
            actual_dep = dep + timedelta(minutes=random.randint(-15, 30))
            actual_arr = None
            odom_start = truck["odometer_miles"] - random.randint(50, 400)
            odom_end = None

            # Interpolate position along route
            orig_t = next(t for t in terminals if t["terminal_id"] == route["origin_terminal_id"])
            dest_t = next(t for t in terminals if t["terminal_id"] == route["destination_terminal_id"])
            elapsed = (now - dep).total_seconds() / 3600
            progress = min(elapsed / drive_h, 0.95)
            cur_lat = round(orig_t["latitude"] + (dest_t["latitude"] - orig_t["latitude"]) * progress, 4)
            cur_lon = round(orig_t["longitude"] + (dest_t["longitude"] - orig_t["longitude"]) * progress, 4)

            load["status"] = "in_transit"
            driver["status"] = "driving"
            truck["status"] = "en_route"
        else:
            # Scheduled trips (future)
            status = "scheduled"
            dep = now + timedelta(hours=random.randint(2, 48))
            drive_h = route["estimated_hours_with_stops"]
            arr = dep + timedelta(hours=drive_h)
            actual_dep = None
            actual_arr = None
            odom_start = None
            odom_end = None
            cur_lat = None
            cur_lon = None
            load["status"] = "assigned"

        trips.append({
            "trip_id": new_id(),
            "trip_number": f"TRP-{70001 + i}",
            "driver_id": driver["driver_id"],
            "truck_id": truck["truck_id"],
            "trailer_id": trailer["trailer_id"],
            "load_id": load["load_id"],
            "route_id": route["route_id"],
            "status": status,
            "scheduled_departure": dep.isoformat() + "Z",
            "scheduled_arrival": arr.isoformat() + "Z",
            "actual_departure": actual_dep.isoformat() + "Z" if actual_dep else None,
            "actual_arrival": actual_arr.isoformat() + "Z" if actual_arr else None,
            "current_latitude": cur_lat,
            "current_longitude": cur_lon,
            "odometer_start": odom_start,
            "odometer_end": odom_end,
        })

    return trips

# ---------------------------------------------------------------------------
# 9. MaintenanceEvents – 100 historical records
# ---------------------------------------------------------------------------

def generate_maintenance_events(trucks, terminals):
    events = []
    maint_terminals = [t for t in terminals if t["has_maintenance_bay"]]
    if not maint_terminals:
        maint_terminals = terminals[:5]

    maint_types_costs = {
        "oil_change":           (400,  800),
        "tire_replacement":     (2000, 5000),
        "brake_inspection":     (300,  1500),
        "dpf_cleaning":         (500,  1200),
        "transmission_service": (1500, 4000),
        "dot_inspection":       (100,  300),
    }

    for i in range(100):
        truck = random.choice(trucks)
        terminal = random.choice(maint_terminals)
        mtype = random.choice(list(maint_types_costs.keys()))
        cost_lo, cost_hi = maint_types_costs[mtype]

        sched_date = random_date(date(2025, 1, 1), date(2026, 2, 14))
        completed = random.random() < 0.85
        comp_date = sched_date + timedelta(days=random.randint(0, 3)) if completed else None

        notes_options = [
            "Routine service completed without issues.",
            "Replaced worn components. Truck returned to service.",
            "Minor issue found during inspection — addressed on site.",
            "Parts on order — follow-up required.",
            "All systems nominal. Passed inspection.",
            "Technician recommends follow-up in 10,000 miles.",
        ]

        events.append({
            "maintenance_event_id": new_id(),
            "truck_id": truck["truck_id"],
            "terminal_id": terminal["terminal_id"],
            "maintenance_type": mtype,
            "status": "completed" if completed else random.choice(["scheduled", "in_progress"]),
            "scheduled_date": str(sched_date),
            "completed_date": str(comp_date) if comp_date else None,
            "odometer_at_service": truck["odometer_miles"] - random.randint(0, 50000),
            "cost_usd": round(random.uniform(cost_lo, cost_hi), 2) if completed else None,
            "technician_notes": random.choice(notes_options) if completed else None,
        })
    return events

# ---------------------------------------------------------------------------
# 10. ServiceTickets – 25 breakdown/repair records
# ---------------------------------------------------------------------------

FAULT_CODES = [
    (110, 0, "Engine Coolant Temperature - Above Normal Operating Range",     "critical"),
    (100, 1, "Engine Oil Pressure - Below Normal Operating Range",            "critical"),
    (111, 0, "Coolant Level - Above Normal Operating Range (Low Level)",      "critical"),
    (157, 0, "Fuel Rail Pressure - Above Normal Operating Range",             "critical"),
    (190, 2, "Engine Speed - Erratic/Intermittent",                           "warning"),
    (94,  1, "Fuel Delivery Pressure - Below Normal Operating Range",         "warning"),
    (91,  3, "Throttle Position - Voltage Above Normal",                      "warning"),
    (84,  2, "Vehicle Speed Sensor - Erratic/Intermittent",                   "warning"),
    (168, 1, "Battery Voltage - Below Normal Operating Range",                "warning"),
    (171, 0, "Ambient Air Temperature - Above Normal Operating Range",        "info"),
]

def generate_service_tickets(trucks, trips, terminals):
    tickets = []
    now = datetime(2026, 3, 17, 12, 0, 0)

    for i in range(25):
        truck = random.choice(trucks)
        trip = random.choice([t for t in trips if t["truck_id"] == truck["truck_id"]] or [None])
        spn, fmi, desc, severity = random.choice(FAULT_CODES)

        # Location: random point in continental US
        lat = round(random.uniform(29.0, 47.0), 4)
        lon = round(random.uniform(-122.0, -75.0), 4)

        reported = now - timedelta(hours=random.randint(1, 720))
        is_resolved = random.random() < 0.7
        resolved = reported + timedelta(hours=random.randint(2, 48)) if is_resolved else None

        if is_resolved:
            status = "closed"
        elif random.random() < 0.5:
            status = "in_progress"
        else:
            status = random.choice(["open", "dispatched"])

        repair_notes_options = [
            "Replaced faulty sensor. Road test passed.",
            "Temporary repair performed. Truck routed to nearest terminal.",
            "Component replaced under warranty.",
            "Cleared fault code after inspection — intermittent issue.",
            "Major repair required. Truck towed to terminal.",
            None,
        ]

        tickets.append({
            "service_ticket_id": new_id(),
            "ticket_number": f"SVC-{40001 + i}",
            "truck_id": truck["truck_id"],
            "trip_id": trip["trip_id"] if trip else None,
            "fault_code_spn": spn,
            "fault_code_fmi": fmi,
            "fault_description": desc,
            "severity": severity,
            "status": status,
            "reported_at": reported.isoformat() + "Z",
            "resolved_at": resolved.isoformat() + "Z" if resolved else None,
            "latitude": lat,
            "longitude": lon,
            "repair_notes": random.choice(repair_notes_options) if is_resolved else None,
            "cost_usd": round(random.uniform(200, 8000), 2) if is_resolved else None,
        })
    return tickets

# ---------------------------------------------------------------------------
# 11. DriverHOSLogs – 500 ELD records (7-day rolling for active drivers)
# ---------------------------------------------------------------------------

DUTY_STATUSES = ["driving", "on_duty_not_driving", "sleeper_berth", "off_duty"]

def generate_hos_logs(drivers, trips):
    logs = []
    now = datetime(2026, 3, 17, 12, 0, 0)
    seven_days_ago = now - timedelta(days=7)

    active_drivers = [d for d in drivers if d["status"] in ("driving", "available")]
    # Generate ~7-8 logs per driver for 7 days
    for driver in active_drivers[:65]:
        # Find any trip for this driver
        driver_trips = [t for t in trips if t["driver_id"] == driver["driver_id"]]

        # Build a sequence of duty status changes over 7 days
        current_time = seven_days_ago + timedelta(hours=random.randint(0, 8))
        cycle_hours = random.uniform(20, 55)  # hours used in 70hr cycle at start

        daily_driving = 0.0
        daily_duty = 0.0

        num_entries = random.randint(6, 10)
        for j in range(num_entries):
            if current_time >= now:
                break

            status = random.choice(DUTY_STATUSES)

            if status == "driving":
                duration_hours = random.uniform(1.5, 4.5)
            elif status == "on_duty_not_driving":
                duration_hours = random.uniform(0.5, 2.0)
            elif status == "sleeper_berth":
                duration_hours = random.uniform(7.0, 10.0)
            else:  # off_duty
                duration_hours = random.uniform(1.0, 10.0)

            end_time = current_time + timedelta(hours=duration_hours)
            if end_time > now:
                end_time = now
                duration_hours = (end_time - current_time).total_seconds() / 3600

            if status == "driving":
                daily_driving += duration_hours
                daily_duty += duration_hours
                cycle_hours += duration_hours
            elif status == "on_duty_not_driving":
                daily_duty += duration_hours
                cycle_hours += duration_hours
            elif status in ("sleeper_berth", "off_duty") and duration_hours >= 10:
                # Reset daily counters after 10hr rest
                daily_driving = 0.0
                daily_duty = 0.0

            drv_remaining = max(0, 11.0 - daily_driving)
            duty_remaining = max(0, 14.0 - daily_duty)
            cycle_remaining = max(0, 70.0 - cycle_hours)

            # Location: near home terminal or interpolated
            home_t = next((t for t in TERMINAL_DATA
                          if any(term["terminal_id"] == driver["home_terminal_id"]
                                for term in [])), None)
            # Use a random terminal location as approximation
            term_data = random.choice(TERMINAL_DATA)
            lat = term_data[3] + random.uniform(-0.5, 0.5)
            lon = term_data[4] + random.uniform(-0.5, 0.5)

            trip_id = driver_trips[0]["trip_id"] if driver_trips and status == "driving" else None

            logs.append({
                "hos_log_id": new_id(),
                "driver_id": driver["driver_id"],
                "trip_id": trip_id,
                "duty_status": status,
                "start_time": current_time.isoformat() + "Z",
                "end_time": end_time.isoformat() + "Z" if end_time < now else None,
                "start_latitude": round(lat, 4),
                "start_longitude": round(lon, 4),
                "start_odometer": random.randint(100000, 600000),
                "driving_hours_used": round(daily_driving, 2),
                "duty_hours_used": round(daily_duty, 2),
                "cycle_hours_used": round(min(cycle_hours, 70), 2),
                "driving_hours_remaining": round(drv_remaining, 2),
                "duty_hours_remaining": round(duty_remaining, 2),
            })

            current_time = end_time

    return logs

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating long-haul trucking reference data...\n")

    # Generate in dependency order
    terminals = generate_terminals()
    write_jsonl("terminals.jsonl", terminals)

    trucks = generate_trucks(terminals)
    write_jsonl("trucks.jsonl", trucks)

    trailers = generate_trailers(terminals)
    write_jsonl("trailers.jsonl", trailers)

    drivers = generate_drivers(terminals)
    write_jsonl("drivers.jsonl", drivers)

    customers = generate_customers()
    write_jsonl("customers.jsonl", customers)

    routes = generate_routes(terminals)
    write_jsonl("routes.jsonl", routes)

    loads = generate_loads(customers, terminals, routes)
    write_jsonl("loads.jsonl", loads)

    trips = generate_trips(drivers, trucks, trailers, loads, routes, terminals)
    write_jsonl("trips.jsonl", trips)

    maintenance_events = generate_maintenance_events(trucks, terminals)
    write_jsonl("maintenance_events.jsonl", maintenance_events)

    service_tickets = generate_service_tickets(trucks, trips, terminals)
    write_jsonl("service_tickets.jsonl", service_tickets)

    hos_logs = generate_hos_logs(drivers, trips)
    write_jsonl("driver_hos_logs.jsonl", hos_logs)

    print(f"\nDone! All files written to: {os.path.abspath(OUTPUT_DIR)}")

if __name__ == "__main__":
    main()
