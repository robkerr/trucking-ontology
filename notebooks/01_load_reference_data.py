# Fabric notebook: Load Reference Data into Lakehouse Tables
#
# This notebook reads JSONL files from the lh_trucking Lakehouse
# Files/reference_data/ folder and creates Delta tables with proper schemas.
#
# Prerequisites:
#   - Attach this notebook to the "lh_trucking" Lakehouse
#   - Upload the reference_data/*.jsonl files to Files/reference_data/
#
# To run: Execute all cells in order.

# %% [markdown]
# ## Configuration

# %%
LAKEHOUSE_NAME = "lh_trucking"
REFERENCE_DATA_PATH = f"Files/reference_data"

# %% [markdown]
# ## Schema Definitions

# %%
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, ArrayType, LongType
)

SCHEMAS = {
    "terminals": StructType([
        StructField("terminal_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("timezone", StringType(), False),
        StructField("capacity_trucks", IntegerType(), True),
        StructField("has_maintenance_bay", BooleanType(), True),
        StructField("address", StringType(), True),
    ]),

    "trucks": StructType([
        StructField("truck_id", StringType(), False),
        StructField("truck_number", StringType(), False),
        StructField("vin", StringType(), False),
        StructField("make", StringType(), False),
        StructField("model", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("odometer_miles", IntegerType(), False),
        StructField("fuel_capacity_gallons", IntegerType(), True),
        StructField("status", StringType(), False),
        StructField("home_terminal_id", StringType(), False),
        StructField("next_maintenance_miles", IntegerType(), True),
        StructField("next_maintenance_type", StringType(), True),
        StructField("last_dot_inspection_date", StringType(), True),
    ]),

    "trailers": StructType([
        StructField("trailer_id", StringType(), False),
        StructField("trailer_number", StringType(), False),
        StructField("type", StringType(), False),
        StructField("length_ft", IntegerType(), False),
        StructField("max_weight_lbs", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("home_terminal_id", StringType(), False),
        StructField("year", IntegerType(), True),
        StructField("last_inspection_date", StringType(), True),
    ]),

    "drivers": StructType([
        StructField("driver_id", StringType(), False),
        StructField("employee_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("cdl_number", StringType(), False),
        StructField("cdl_state", StringType(), False),
        StructField("cdl_endorsements", ArrayType(StringType()), True),
        StructField("cdl_expiration_date", StringType(), True),
        StructField("hire_date", StringType(), True),
        StructField("status", StringType(), False),
        StructField("home_terminal_id", StringType(), False),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("supervisor_email", StringType(), True),
    ]),

    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("contact_name", StringType(), True),
        StructField("contact_email", StringType(), True),
        StructField("contact_phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("industry", StringType(), True),
    ]),

    "routes": StructType([
        StructField("route_id", StringType(), False),
        StructField("route_name", StringType(), False),
        StructField("origin_terminal_id", StringType(), False),
        StructField("destination_terminal_id", StringType(), False),
        StructField("distance_miles", DoubleType(), False),
        StructField("estimated_hours", DoubleType(), False),
        StructField("estimated_hours_with_stops", DoubleType(), True),
        StructField("waypoints", StringType(), True),
        StructField("toll_cost_estimate", DoubleType(), True),
        StructField("fuel_stops_recommended", IntegerType(), True),
    ]),

    "loads": StructType([
        StructField("load_id", StringType(), False),
        StructField("load_number", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("description", StringType(), True),
        StructField("weight_lbs", IntegerType(), False),
        StructField("required_trailer_type", StringType(), False),
        StructField("required_endorsements", ArrayType(StringType()), True),
        StructField("pickup_terminal_id", StringType(), False),
        StructField("delivery_terminal_id", StringType(), False),
        StructField("pickup_window_start", StringType(), True),
        StructField("pickup_window_end", StringType(), True),
        StructField("delivery_window_start", StringType(), True),
        StructField("delivery_window_end", StringType(), True),
        StructField("status", StringType(), False),
        StructField("priority", StringType(), True),
        StructField("value_usd", DoubleType(), True),
    ]),

    "trips": StructType([
        StructField("trip_id", StringType(), False),
        StructField("trip_number", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("truck_id", StringType(), False),
        StructField("trailer_id", StringType(), False),
        StructField("load_id", StringType(), False),
        StructField("route_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("scheduled_departure", StringType(), True),
        StructField("scheduled_arrival", StringType(), True),
        StructField("actual_departure", StringType(), True),
        StructField("actual_arrival", StringType(), True),
        StructField("current_latitude", DoubleType(), True),
        StructField("current_longitude", DoubleType(), True),
        StructField("odometer_start", IntegerType(), True),
        StructField("odometer_end", IntegerType(), True),
    ]),

    "maintenance_events": StructType([
        StructField("maintenance_event_id", StringType(), False),
        StructField("truck_id", StringType(), False),
        StructField("terminal_id", StringType(), False),
        StructField("maintenance_type", StringType(), False),
        StructField("status", StringType(), False),
        StructField("scheduled_date", StringType(), True),
        StructField("completed_date", StringType(), True),
        StructField("odometer_at_service", IntegerType(), True),
        StructField("cost_usd", DoubleType(), True),
        StructField("technician_notes", StringType(), True),
    ]),

    "service_tickets": StructType([
        StructField("service_ticket_id", StringType(), False),
        StructField("ticket_number", StringType(), False),
        StructField("truck_id", StringType(), False),
        StructField("trip_id", StringType(), True),
        StructField("fault_code_spn", IntegerType(), False),
        StructField("fault_code_fmi", IntegerType(), False),
        StructField("fault_description", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("reported_at", StringType(), True),
        StructField("resolved_at", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("repair_notes", StringType(), True),
        StructField("cost_usd", DoubleType(), True),
    ]),

    "driver_hos_logs": StructType([
        StructField("hos_log_id", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("trip_id", StringType(), True),
        StructField("duty_status", StringType(), False),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("start_latitude", DoubleType(), True),
        StructField("start_longitude", DoubleType(), True),
        StructField("start_odometer", IntegerType(), True),
        StructField("driving_hours_used", DoubleType(), True),
        StructField("duty_hours_used", DoubleType(), True),
        StructField("cycle_hours_used", DoubleType(), True),
        StructField("driving_hours_remaining", DoubleType(), True),
        StructField("duty_hours_remaining", DoubleType(), True),
    ]),
}

# Primary key column for each table
PRIMARY_KEYS = {
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

# %% [markdown]
# ## Load JSONL Files into Lakehouse Tables

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Order matters: load tables with no FK dependencies first
LOAD_ORDER = [
    "terminals",
    "customers",
    "trucks",
    "trailers",
    "drivers",
    "routes",
    "loads",
    "trips",
    "maintenance_events",
    "service_tickets",
    "driver_hos_logs",
]

results = []

for table_name in LOAD_ORDER:
    file_path = f"{REFERENCE_DATA_PATH}/{table_name}.jsonl"
    schema = SCHEMAS[table_name]
    pk = PRIMARY_KEYS[table_name]

    try:
        # Read JSONL (JSON Lines format = one JSON object per line)
        df = spark.read.schema(schema).json(file_path)

        # Verify no null primary keys
        null_pk_count = df.filter(df[pk].isNull()).count()
        if null_pk_count > 0:
            print(f"⚠ WARNING: {table_name} has {null_pk_count} null primary keys ({pk})")

        # Verify primary key uniqueness
        total = df.count()
        distinct_pk = df.select(pk).distinct().count()
        if total != distinct_pk:
            print(f"⚠ WARNING: {table_name} has {total - distinct_pk} duplicate primary keys ({pk})")

        # Write as Delta table (overwrite to support re-runs)
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        results.append({"table": table_name, "rows": total, "pk": pk, "status": "✓ loaded"})
        print(f"  ✓ {table_name}: {total} rows loaded (PK: {pk})")

    except Exception as e:
        results.append({"table": table_name, "rows": 0, "pk": pk, "status": f"✗ {str(e)[:80]}"})
        print(f"  ✗ {table_name}: FAILED — {e}")

# %% [markdown]
# ## Summary

# %%
from pyspark.sql import Row

summary_df = spark.createDataFrame([Row(**r) for r in results])
display(summary_df)

# %% [markdown]
# ## Verification: Row Counts & Sample Data

# %%
print("=" * 60)
print("TABLE ROW COUNTS")
print("=" * 60)
for table_name in LOAD_ORDER:
    try:
        count = spark.table(table_name).count()
        print(f"  {table_name:<25} {count:>6} rows")
    except Exception as e:
        print(f"  {table_name:<25} ERROR: {e}")

# %%
# Display sample records from key tables
for table_name in ["terminals", "trucks", "drivers", "trips"]:
    print(f"\n{'=' * 60}")
    print(f"SAMPLE: {table_name} (first 3 rows)")
    print("=" * 60)
    display(spark.table(table_name).limit(3))
