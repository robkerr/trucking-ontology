# Long-Haul Trucking Ontology — Microsoft Fabric Demo

A demo solution implementing a long-haul trucking ontology with Microsoft Fabric. Includes synthetic reference data, event stream generation, and five operational scenarios.

## Demo Scenarios

| Scenario | Trigger | Action |
|---|---|---|
| **Late Arrival** | ETA exceeds scheduled arrival window | Email notification to customer |
| **Breakdown** | Critical engine fault code while en route | Dispatch service truck |
| **HOS Violation Risk** | Driver approaching hours-of-service limits | Email notification to supervisor |
| **Maintenance Due** | Truck odometer crosses maintenance threshold | POST to maintenance scheduling API |
| **Load Reassignment** | Trip becomes infeasible (breakdown/HOS) | Find alternate truck+driver for load |

## Ontology Entities

### Reference Data (11 entities)
- **Terminal** — 15 real US trucking hub cities with lat/lon
- **Truck** — 50 tractor units (Freightliner, Kenworth, Peterbilt, Volvo, International, Mack)
- **Trailer** — 60 trailers (dry van, reefer, flatbed, tanker)
- **Driver** — 65 CDL holders with endorsements (H, N, T, X)
- **Customer** — 20 shipping companies
- **Route** — 40 predefined lanes between terminals with realistic distances
- **Load** — 30 freight loads (general, hazmat, refrigerated, oversize)
- **Trip** — 30 dispatch records linking driver + truck + trailer + load + route
- **MaintenanceEvent** — 100 historical maintenance records
- **ServiceTicket** — 25 breakdown/repair records
- **DriverHOSLog** — 500 ELD duty status records (7-day history)

### Event Streams (5 types → Event Hub)
- **TelemetryEvent** — GPS, speed, fuel, engine temp every 30s per active truck
- **EngineFaultEvent** — J1939 SPN/FMI diagnostic trouble codes
- **GeofenceEvent** — Terminal arrival/departure detection
- **HOSStatusChangeEvent** — Driver duty status transitions
- **LoadStatusEvent** — Load pickup/transit/delivery status changes

## Project Structure

```
reference_data/          # JSONL files (one JSON object per line)
scripts/
  generate_reference_data.py   # Generates all JSONL reference data
notebooks/
  01_load_reference_data.py    # Fabric notebook: load JSONL → Lakehouse tables
  02_generate_events.py        # Fabric notebook: generate event streams → Event Hub
schemas/
  ontology.md                  # Entity definitions & relationships
  event_schemas.md             # Event type JSON schemas
```

## Getting Started

### 1. Generate Reference Data
```bash
cd scripts
python generate_reference_data.py
```
This produces 11 JSONL files in the `reference_data/` folder.

### 2. Load into Fabric Lakehouse
Upload the `reference_data/` folder to your **lh_trucking** Lakehouse under `Files/reference_data/`, then run the `01_load_reference_data` notebook.

### 3. Generate Event Streams
Configure your Event Hub connection string in the `02_generate_events` notebook and run it to produce synthetic event data.

## Key Realism Details

- **HOS Rules**: FMCSA compliant — 11hr driving, 14hr on-duty, 30min break after 8hr, 70hr/8-day cycle
- **Fault Codes**: Real SAE J1939 SPN/FMI codes
- **Maintenance**: Industry-standard intervals (oil 25K mi, tires 50K mi, brakes 30K mi, DOT annual)
- **Geography**: Real US city coordinates for geospatial analysis
- **CDL Endorsements**: Hazmat (H), Tanker (N), Doubles/Triples (T), Combo (X)
