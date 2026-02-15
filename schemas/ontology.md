# Ontology Schema — Long-Haul Trucking

## Entity Definitions

---

### Terminal
Physical hub/yard location where trucks are dispatched, loaded, and serviced.

| Field | Type | Description |
|---|---|---|
| terminal_id | UUID (PK) | Unique identifier |
| name | string | Terminal name (e.g., "Atlanta Hub") |
| city | string | City name |
| state | string | Two-letter state code |
| latitude | float | GPS latitude |
| longitude | float | GPS longitude |
| timezone | string | IANA timezone (e.g., "America/New_York") |
| capacity_trucks | int | Max truck parking capacity |
| has_maintenance_bay | boolean | Whether maintenance can be performed here |
| address | string | Street address |

---

### Truck
Tractor unit (Class 8 semi-truck).

| Field | Type | Description |
|---|---|---|
| truck_id | UUID (PK) | Unique identifier |
| truck_number | string | Fleet number (e.g., "TRK-1042") |
| vin | string | 17-character Vehicle Identification Number |
| make | string | Manufacturer (Freightliner, Kenworth, etc.) |
| model | string | Model name (Cascadia, T680, etc.) |
| year | int | Model year |
| odometer_miles | int | Current odometer reading |
| fuel_capacity_gallons | float | Fuel tank capacity |
| status | string | available, en_route, maintenance, out_of_service |
| home_terminal_id | UUID (FK → Terminal) | Assigned home terminal |
| next_maintenance_miles | int | Odometer reading for next scheduled maintenance |
| next_maintenance_type | string | Type of next maintenance due |
| last_dot_inspection_date | date | Date of last DOT inspection |

---

### Trailer
Trailer unit pulled by a tractor.

| Field | Type | Description |
|---|---|---|
| trailer_id | UUID (PK) | Unique identifier |
| trailer_number | string | Fleet number (e.g., "TRL-2001") |
| type | string | dry_van, reefer, flatbed, tanker |
| length_ft | int | Trailer length (48 or 53 ft) |
| max_weight_lbs | int | Maximum cargo weight |
| status | string | available, loaded, en_route, maintenance |
| home_terminal_id | UUID (FK → Terminal) | Assigned home terminal |
| year | int | Model year |
| last_inspection_date | date | Date of last inspection |

---

### Driver
CDL-holding truck driver.

| Field | Type | Description |
|---|---|---|
| driver_id | UUID (PK) | Unique identifier |
| employee_id | string | Employee number (e.g., "DRV-5001") |
| first_name | string | First name |
| last_name | string | Last name |
| cdl_number | string | Commercial Driver's License number |
| cdl_state | string | State of CDL issuance |
| cdl_endorsements | string[] | List of endorsements: H (hazmat), N (tanker), T (doubles/triples), X (hazmat+tanker) |
| cdl_expiration_date | date | CDL expiration date |
| hire_date | date | Date of hire |
| status | string | available, driving, off_duty, on_leave |
| home_terminal_id | UUID (FK → Terminal) | Assigned home terminal |
| phone | string | Contact phone number |
| email | string | Contact email |
| supervisor_email | string | Supervisor's email (for HOS alerts) |

---

### Customer
Shipping customer who contracts loads.

| Field | Type | Description |
|---|---|---|
| customer_id | UUID (PK) | Unique identifier |
| name | string | Company name |
| contact_name | string | Primary contact person |
| contact_email | string | Contact email |
| contact_phone | string | Contact phone |
| address | string | Business address |
| city | string | City |
| state | string | State |
| industry | string | Industry vertical (manufacturing, retail, agriculture, etc.) |

---

### Route
Predefined shipping lane between two terminals.

| Field | Type | Description |
|---|---|---|
| route_id | UUID (PK) | Unique identifier |
| route_name | string | Human-readable name (e.g., "ATL→CHI") |
| origin_terminal_id | UUID (FK → Terminal) | Starting terminal |
| destination_terminal_id | UUID (FK → Terminal) | Ending terminal |
| distance_miles | float | Total route distance |
| estimated_hours | float | Estimated driving time (not including breaks) |
| estimated_hours_with_stops | float | Estimated total time including fuel/rest stops |
| waypoints | string (JSON) | JSON array of intermediate lat/lon waypoints |
| toll_cost_estimate | float | Estimated toll costs |
| fuel_stops_recommended | int | Recommended number of fuel stops |

---

### Load
A shipment of freight to be transported.

| Field | Type | Description |
|---|---|---|
| load_id | UUID (PK) | Unique identifier |
| load_number | string | Reference number (e.g., "LD-90001") |
| customer_id | UUID (FK → Customer) | Customer who owns the freight |
| load_type | string | general, hazmat, refrigerated, oversize |
| description | string | Cargo description |
| weight_lbs | int | Cargo weight |
| required_trailer_type | string | Required trailer type (dry_van, reefer, flatbed, tanker) |
| required_endorsements | string[] | Required CDL endorsements for this load |
| pickup_terminal_id | UUID (FK → Terminal) | Pickup location |
| delivery_terminal_id | UUID (FK → Terminal) | Delivery location |
| pickup_window_start | datetime | Earliest pickup time |
| pickup_window_end | datetime | Latest pickup time |
| delivery_window_start | datetime | Earliest delivery time |
| delivery_window_end | datetime | Latest delivery time |
| status | string | pending, assigned, in_transit, delivered, cancelled |
| priority | string | standard, expedited, critical |
| value_usd | float | Declared cargo value |

---

### Trip
Operational dispatch record linking all entities for a single haul.

| Field | Type | Description |
|---|---|---|
| trip_id | UUID (PK) | Unique identifier |
| trip_number | string | Reference number (e.g., "TRP-70001") |
| driver_id | UUID (FK → Driver) | Assigned driver |
| truck_id | UUID (FK → Truck) | Assigned tractor |
| trailer_id | UUID (FK → Trailer) | Assigned trailer |
| load_id | UUID (FK → Load) | Freight being hauled |
| route_id | UUID (FK → Route) | Route being followed |
| status | string | scheduled, in_progress, completed, cancelled, interrupted |
| scheduled_departure | datetime | Planned departure time |
| scheduled_arrival | datetime | Planned arrival time |
| actual_departure | datetime | Actual departure time (null if not started) |
| actual_arrival | datetime | Actual arrival time (null if not completed) |
| current_latitude | float | Last known latitude (null if not started) |
| current_longitude | float | Last known longitude (null if not started) |
| odometer_start | int | Truck odometer at trip start |
| odometer_end | int | Truck odometer at trip end (null if in progress) |

---

### MaintenanceEvent
Scheduled or completed maintenance on a truck.

| Field | Type | Description |
|---|---|---|
| maintenance_event_id | UUID (PK) | Unique identifier |
| truck_id | UUID (FK → Truck) | Truck being serviced |
| terminal_id | UUID (FK → Terminal) | Terminal where maintenance performed |
| maintenance_type | string | oil_change, tire_replacement, brake_inspection, dpf_cleaning, transmission_service, dot_inspection |
| status | string | scheduled, in_progress, completed, cancelled |
| scheduled_date | date | Planned date |
| completed_date | date | Actual completion date |
| odometer_at_service | int | Odometer reading at time of service |
| cost_usd | float | Maintenance cost |
| technician_notes | string | Service notes |

---

### ServiceTicket
Breakdown or roadside repair record.

| Field | Type | Description |
|---|---|---|
| service_ticket_id | UUID (PK) | Unique identifier |
| ticket_number | string | Reference number (e.g., "SVC-40001") |
| truck_id | UUID (FK → Truck) | Affected truck |
| trip_id | UUID (FK → Trip) | Trip during which breakdown occurred (nullable) |
| fault_code_spn | int | J1939 SPN (Suspect Parameter Number) |
| fault_code_fmi | int | J1939 FMI (Failure Mode Identifier) |
| fault_description | string | Human-readable fault description |
| severity | string | info, warning, critical |
| status | string | open, dispatched, in_progress, resolved, closed |
| reported_at | datetime | When the fault was reported |
| resolved_at | datetime | When the issue was resolved |
| latitude | float | Location where breakdown occurred |
| longitude | float | Location where breakdown occurred |
| repair_notes | string | Technician repair notes |
| cost_usd | float | Repair cost |

---

### DriverHOSLog
Electronic Logging Device (ELD) duty status record per FMCSA regulations.

| Field | Type | Description |
|---|---|---|
| hos_log_id | UUID (PK) | Unique identifier |
| driver_id | UUID (FK → Driver) | Driver |
| trip_id | UUID (FK → Trip) | Associated trip (nullable if off-duty) |
| duty_status | string | driving, on_duty_not_driving, sleeper_berth, off_duty |
| start_time | datetime | Status period start |
| end_time | datetime | Status period end (null if current) |
| start_latitude | float | Location at status start |
| start_longitude | float | Location at status start |
| start_odometer | int | Odometer at status start |
| driving_hours_used | float | Cumulative driving hours in current 14hr window |
| duty_hours_used | float | Cumulative on-duty hours in current 14hr window |
| cycle_hours_used | float | Cumulative hours in 70hr/8-day cycle |
| driving_hours_remaining | float | Hours of driving time remaining |
| duty_hours_remaining | float | Hours of on-duty time remaining |

---

## Relationship Diagram

```
Terminal ←── home_terminal_id ──── Truck
Terminal ←── home_terminal_id ──── Trailer
Terminal ←── home_terminal_id ──── Driver
Terminal ←── origin_terminal_id ── Route
Terminal ←── dest_terminal_id ──── Route
Terminal ←── pickup_terminal_id ── Load
Terminal ←── delivery_terminal_id  Load
Terminal ←── terminal_id ───────── MaintenanceEvent
Customer ←── customer_id ───────── Load

Trip ───→ Driver    (driver_id)
Trip ───→ Truck     (truck_id)
Trip ───→ Trailer   (trailer_id)
Trip ───→ Load      (load_id)
Trip ───→ Route     (route_id)

MaintenanceEvent ───→ Truck (truck_id)
ServiceTicket ──────→ Truck (truck_id)
ServiceTicket ──────→ Trip  (trip_id)
DriverHOSLog ───────→ Driver (driver_id)
DriverHOSLog ───────→ Trip   (trip_id)
```
