# Event Stream Schemas — Long-Haul Trucking

All events are structured as JSON for posting to Microsoft Fabric Event Hub. Each event includes a standard envelope with event-specific payload.

## Common Envelope

```json
{
  "event_id": "UUID",
  "event_type": "string",
  "timestamp": "ISO 8601 datetime",
  "source": "string (system identifier)",
  "body": { ... }
}
```

---

## 1. TelemetryEvent

High-frequency GPS and vehicle telemetry from onboard telematics unit. Emitted every 30 seconds per active truck.

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "event_type": "TelemetryEvent",
  "timestamp": "2026-02-15T14:30:00Z",
  "source": "telematics-unit",
  "body": {
    "truck_id": "UUID",
    "trip_id": "UUID",
    "driver_id": "UUID",
    "latitude": 34.0522,
    "longitude": -118.2437,
    "speed_mph": 62.5,
    "heading_degrees": 275,
    "fuel_pct": 68.3,
    "engine_temp_f": 198,
    "oil_pressure_psi": 42,
    "odometer_miles": 452103,
    "engine_rpm": 1450,
    "ambient_temp_f": 72,
    "def_level_pct": 85.0
  }
}
```

| Field | Type | Range | Description |
|---|---|---|---|
| truck_id | UUID | — | Truck transmitting telemetry |
| trip_id | UUID | — | Current trip (null if deadheading) |
| driver_id | UUID | — | Current driver |
| latitude | float | -90 to 90 | GPS latitude |
| longitude | float | -180 to 180 | GPS longitude |
| speed_mph | float | 0–85 | Current speed |
| heading_degrees | int | 0–359 | Compass heading |
| fuel_pct | float | 0–100 | Fuel level percentage |
| engine_temp_f | int | 150–250 | Engine coolant temperature |
| oil_pressure_psi | int | 20–80 | Engine oil pressure |
| odometer_miles | int | 0+ | Cumulative odometer |
| engine_rpm | int | 0–2500 | Engine RPM |
| ambient_temp_f | int | -20 to 120 | Outside air temperature |
| def_level_pct | float | 0–100 | Diesel Exhaust Fluid level |

---

## 2. EngineFaultEvent

Diagnostic trouble code from the truck's J1939 CAN bus. Emitted when a fault code is set or cleared.

```json
{
  "event_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "event_type": "EngineFaultEvent",
  "timestamp": "2026-02-15T14:32:15Z",
  "source": "engine-ecu",
  "body": {
    "truck_id": "UUID",
    "trip_id": "UUID",
    "driver_id": "UUID",
    "spn": 110,
    "fmi": 0,
    "fault_description": "Engine Coolant Temperature - Data Valid But Above Normal Operating Range",
    "severity": "critical",
    "occurrence_count": 1,
    "latitude": 34.0522,
    "longitude": -118.2437,
    "action": "set"
  }
}
```

| Field | Type | Description |
|---|---|---|
| truck_id | UUID | Truck reporting fault |
| trip_id | UUID | Current trip (nullable) |
| driver_id | UUID | Current driver |
| spn | int | SAE J1939 Suspect Parameter Number |
| fmi | int | Failure Mode Identifier |
| fault_description | string | Human-readable fault description |
| severity | string | info, warning, critical |
| occurrence_count | int | Number of times fault has occurred |
| latitude | float | Location when fault occurred |
| longitude | float | Location when fault occurred |
| action | string | "set" (new fault) or "cleared" (fault resolved) |

### Reference: Common J1939 Fault Codes

| SPN | FMI | Description | Severity |
|---|---|---|---|
| 110 | 0 | Engine Coolant Temp — Above Normal | critical |
| 100 | 1 | Engine Oil Pressure — Below Normal | critical |
| 111 | 0 | Coolant Level — Above Normal (Low) | critical |
| 157 | 0 | Fuel Rail Pressure — Above Normal | critical |
| 190 | 2 | Engine Speed — Erratic/Intermittent | warning |
| 94 | 1 | Fuel Delivery Pressure — Below Normal | warning |
| 91 | 3 | Throttle Position — Voltage Above Normal | warning |
| 84 | 2 | Vehicle Speed — Erratic/Intermittent | warning |
| 168 | 1 | Battery Voltage — Below Normal | warning |
| 171 | 0 | Ambient Air Temp — Above Normal | info |

---

## 3. GeofenceEvent

Emitted when a truck enters or exits a terminal geofence boundary (typically 0.5-mile radius).

```json
{
  "event_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
  "event_type": "GeofenceEvent",
  "timestamp": "2026-02-15T18:45:00Z",
  "source": "geofence-service",
  "body": {
    "truck_id": "UUID",
    "trip_id": "UUID",
    "driver_id": "UUID",
    "terminal_id": "UUID",
    "terminal_name": "Chicago Hub",
    "geofence_event": "enter",
    "latitude": 41.8781,
    "longitude": -87.6298
  }
}
```

| Field | Type | Description |
|---|---|---|
| truck_id | UUID | Truck crossing geofence |
| trip_id | UUID | Current trip |
| driver_id | UUID | Current driver |
| terminal_id | UUID | Terminal whose geofence was crossed |
| terminal_name | string | Terminal name for readability |
| geofence_event | string | "enter" or "exit" |
| latitude | float | Exact location at crossing |
| longitude | float | Exact location at crossing |

---

## 4. HOSStatusChangeEvent

Emitted when a driver's duty status changes per ELD (Electronic Logging Device) rules.

```json
{
  "event_id": "d4e5f6a7-b8c9-0123-defa-234567890123",
  "event_type": "HOSStatusChangeEvent",
  "timestamp": "2026-02-15T06:00:00Z",
  "source": "eld-device",
  "body": {
    "driver_id": "UUID",
    "trip_id": "UUID",
    "truck_id": "UUID",
    "previous_status": "sleeper_berth",
    "new_status": "driving",
    "driving_hours_used": 0.0,
    "driving_hours_remaining": 11.0,
    "duty_hours_used": 0.0,
    "duty_hours_remaining": 14.0,
    "cycle_hours_used": 42.5,
    "cycle_hours_remaining": 27.5,
    "break_time_remaining_minutes": 480,
    "latitude": 33.749,
    "longitude": -84.388
  }
}
```

| Field | Type | Description |
|---|---|---|
| driver_id | UUID | Driver changing status |
| trip_id | UUID | Current trip (nullable if off-duty) |
| truck_id | UUID | Current truck |
| previous_status | string | driving, on_duty_not_driving, sleeper_berth, off_duty |
| new_status | string | Same enum as above |
| driving_hours_used | float | Hours driven in current window |
| driving_hours_remaining | float | Driving hours left before 11hr limit |
| duty_hours_used | float | On-duty hours in current 14hr window |
| duty_hours_remaining | float | Hours left in 14hr window |
| cycle_hours_used | float | Hours used in 70hr/8-day cycle |
| cycle_hours_remaining | float | Hours left in 70hr cycle |
| break_time_remaining_minutes | int | Minutes until 30-min break required |
| latitude | float | Location at status change |
| longitude | float | Location at status change |

### HOS Rules Reference (FMCSA)
- **11-Hour Driving Limit**: Max 11 hours driving after 10 consecutive hours off duty
- **14-Hour Window**: Cannot drive beyond 14th consecutive hour after coming on duty
- **30-Minute Break**: Required after 8 cumulative hours of driving
- **70-Hour/8-Day Limit**: Cannot drive after 70 hours on duty in 8 consecutive days
- **34-Hour Restart**: Can reset 70-hour clock with 34 consecutive hours off duty

---

## 5. LoadStatusEvent

Emitted when a load's status changes through its lifecycle.

```json
{
  "event_id": "e5f6a7b8-c9d0-1234-efab-345678901234",
  "event_type": "LoadStatusEvent",
  "timestamp": "2026-02-15T08:15:00Z",
  "source": "dispatch-system",
  "body": {
    "load_id": "UUID",
    "trip_id": "UUID",
    "customer_id": "UUID",
    "load_number": "LD-90001",
    "previous_status": "pending",
    "new_status": "in_transit",
    "terminal_id": "UUID",
    "latitude": 33.749,
    "longitude": -84.388,
    "estimated_arrival": "2026-02-16T14:00:00Z",
    "notes": "Load picked up, departing Atlanta Hub"
  }
}
```

| Field | Type | Description |
|---|---|---|
| load_id | UUID | Load changing status |
| trip_id | UUID | Associated trip |
| customer_id | UUID | Customer who owns the load |
| load_number | string | Human-readable load reference |
| previous_status | string | pending, assigned, in_transit, delivered, cancelled |
| new_status | string | Same enum as above |
| terminal_id | UUID | Terminal where status changed (nullable if en route) |
| latitude | float | Location at status change |
| longitude | float | Location at status change |
| estimated_arrival | datetime | Updated ETA (nullable) |
| notes | string | Optional notes |
