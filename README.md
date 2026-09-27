# Smart Home Lighting System — A Scalable IoT Solution

**SIT314 — IoT Systems | Distinction Task | Akshit Bhullar**

A multi-tenant smart lighting system that controls lighting automatically based on occupancy, ambient light, schedules and manual override — built as an event-driven, auto-scaling architecture on AWS.

## Architecture

```
Device Simulators (Node.js)
        │  MQTT
        ▼
Local Mosquitto Broker
        │
        ▼
Node-RED (filtering & aggregation)
  ├─ Debounce (motion)
  ├─ Threshold + hysteresis (light)
  └─ Sliding-window aggregation (energy)
        │
        ▼
Edge Gateway (Node.js, mTLS)
        │
        ▼
AWS IoT Core
        │
        ▼
Amazon SNS (event bus)
        │
   ┌────┴────┐
   ▼         ▼
SQS Queue   SQS Queue
   │         │
   ▼         ▼
Lambda:     Lambda:
Lighting    Notification
Control     (anomaly detection)
   │         │
   └────┬────┘
        ▼
   DynamoDB (device state)
```

## What's implemented

- **Data collection (simulated)**: 4 Node.js scripts simulating a motion sensor, ambient light sensor, smart switch, and energy meter — each publishing realistic MQTT payloads (`simulators/`).
- **Flow-based processing (Node-RED)**: debouncing, threshold + hysteresis, and sliding-window aggregation, matching the algorithms described in the project plan.
- **Secure edge-to-cloud bridge**: `edge_gateway.js` forwards data from the local broker to AWS IoT Core over mutual TLS, using a device certificate scoped to a least-privilege IoT policy (`certs/iot-policy-scoped.json`).
- **Event-driven microservices**: an SNS topic (`SmartLightingEvents`) fans out to per-service SQS queues, decoupling ingestion from processing.
  - `services/lighting-control-lambda.js` — actuates lighting commands and writes device state to DynamoDB.
  - `services/notification-lambda.js` — tracks occupancy and flags anomalies (light on with no occupancy for >2 hours).
- **Deployed on AWS Lambda**, triggered directly by SQS — Lambda automatically scales concurrent execution count with queue depth, with no manual server management.
- **Storage**: DynamoDB (`DeviceState` table, on-demand billing) for device state.
- **Security**: mutual TLS on all device connections; least-privilege IAM policy for the project's AWS user (no admin access); least-privilege IoT policy scoping each device certificate to its own property's topics.

## Scalability testing

Load was generated with `load_test.js`, publishing bursts of SNS messages, while `monitor_queue.js` sampled SQS queue depth every second to observe backlog behaviour.

**Before scaling** (SQS→Lambda batch size = 1): a 2,000-message burst produced a backlog peaking at ~1,970 waiting messages, taking approximately **43 seconds** to fully drain.

**After scaling** (batch size increased to 10): the identical 2,000-message burst peaked lower and fully drained in approximately **16 seconds** — roughly **2.7x faster**, with the system sustaining a much higher concurrent in-flight message count (248 vs 89 at peak).

A separate test also identified that the AWS account's default Lambda concurrency limit (10) causes a real bottleneck under heavier load (5,000 messages produced a backlog of up to 3,282 messages); a service quota increase to 1,000 concurrent executions was requested via AWS Support to allow further scaling headroom.

Full test output and screenshots are in `evidence/`.

## Running it locally

```bash
npm install
node simulators/motion_sensor.js      # separate terminal
node simulators/light_sensor.js       # separate terminal
node simulators/smart_switch.js       # separate terminal
node simulators/energy_meter.js       # separate terminal
node edge_gateway.js                  # separate terminal
node-red                              # separate terminal, then import the flow
```

Requires a local Mosquitto broker running on `localhost:1883`, and AWS credentials configured (`aws configure`) with access to the IoT Core, SNS, SQS, Lambda, and DynamoDB resources referenced in the scripts.

## Project plan

See [`docs/distinction_plan.pdf`](docs/distinction_plan.pdf) for the full requirements analysis, data flow diagram, testing plan, and implementation plan.

## High Distinction extension: edge aggregation

Distinction testing found that a near-instantaneous 5,000-message burst got *slower* after Lambda-side scaling, because Lambda's SQS pollers start at 5 concurrent invokes and scale up gradually. The HD work tackles this at the source by reducing the number of messages the cloud has to handle:

- `edge/batcher.js` — edge aggregator: coalesces device events into batch envelopes, flushing on size (`maxBatch`, default 25), time (`maxWaitMs`, default 100ms) or immediately for urgent events (manual switch overrides).
- `edge_gateway.js` — uses the batcher by default (`BATCHING=off` restores per-message forwarding); envelopes go to `bridge/smarthome/<propertyId>/batch`, which the existing IoT Rule already routes.
- `lambda-deploy/index.js` — LightingControlFunction accepts raw events or envelopes, writes state with DynamoDB `BatchWriteItem` (25 per call, retries unprocessed items), reports partial batch failures, and logs per-event latency (`[LATENCY] runId=...`) plus a per-invocation summary (`[INVOCATION] ...`).
- `notification-deploy/`, `scheduling-deploy/` — unwrap envelopes so they keep working on batched traffic.
- `load_test.js` — `--mode raw|batched`, `--batch`, `--wait`, `--duration` (sustained load), `--run` (run ID).
- `latency_report.js` — pulls p50/p95/p99, invocation count and failures for a run from CloudWatch Logs Insights.
- `run_experiments.js` — runs the full experiment matrix and writes `results/runs.jsonl` + `results/latency.jsonl`.
