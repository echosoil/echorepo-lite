# ECHOREPO Architecture Overview

## 1 · Big Picture

ECHOREPO links a field‑data mobile application with a Docker‑based micro‑service backend, using cloud‑synchronisation for offline resilience and a unified authentication layer.

```
┌────────────┐        Near‑real‑time Sync        ┌───────────────┐   JWT   ┌──────────────┐
│ Mobile App │ ◀──────────────────────────────▶ │  Firebase RTDB │ ◀──────▶│  API Gateway │
└────────────┘                                   └───────────────┘         │(Envoy / NGINX)│
     ▲  ▲  OIDC                                                      mTLS   └──────▲───────┘
     │  │                                                         ┌────────┴────────┐
     │  └─────── entity["organization","Keycloak","open‑source iam"]  ──────────────▶ │ Ingestion SVC │
     │                                               ┌───────────▶│ Kit‑Registry │
     │                                               │            │ Data‑Proc   │
     │                                               │            └────┬────────┘
     │                                               │                 │
     └───────────────────── Web UI / Dashboards ◀────┴─────── Time‑series / SQL
```

**Flow (happy‑path)**
1. User authenticates through the mobile app; OIDC tokens are issued by Keycloak.
2. Measurements and kit metadata are cached locally and synced to entity["organization","Google Firebase","baas platform"] when connectivity is present.
3. A Cloud Function (sync bridge) streams changes into the backend via the gateway.
4. Micro‑services ingest, enrich, and store data, emitting events on a message bus.
5. The same REST/GraphQL API powers dashboards and third‑party integrations.

---

## 2 · Component Breakdown

| Layer | Core Components | Technology Stack | Responsibility |
|-------|-----------------|------------------|----------------|
| **Client** | Mobile App (Android/iOS), QR‑scanner, offline cache | Flutter / React Native | Capture measurements, scan kit IDs, operate offline. |
| **Sync & BaaS** | Firebase Realtime DB, Cloud Functions | Managed GCP service | Low‑latency sync, triggers to backend, push notifications. |
| **Identity & Access** | Keycloak realm, OIDC tokens, role mappings | Self‑hosted container | SSO, MFA, issuing JWTs, role/kit‑claim policies. |
| **Gateway / Edge** | Envoy or NGINX Ingress, rate‑limit, metrics | Docker / K8s | TLS termination, routing, token verification, circuit‑breaking. |
| **Core Micro‑services** | Ingestion, Kit‑Registry, Data‑Processor, Visualisation API | FastAPI / Go, gRPC/REST | Validate payloads, store data, business rules, expose rich queries. |
| **Data Stores** | PostgreSQL (metadata), TimescaleDB or InfluxDB (sensor data), Object Storage (media) | Helm‑deployed DBs, S3‑compatible storage | Durable storage, retention policies, backups. |
| **Web Front‑end** | React/Vue SPA, Dashboards, Map overlays | TypeScript, D3/Leaflet | Visualise datasets, export reports, admin console. |
| **Messaging & Events** | Kafka or RabbitMQ bus | Strimzi / Bitnami chart | Decouple ingestion from heavy processing and analytics. |
| **Observability** | Prometheus, Grafana, Loki/ELK | CNCF stack | Logs, metrics, traces, alerting. |
| **CI/CD & Ops** | GitHub Actions, Docker registry, Helm charts | Kubernetes (EKS/AKS/GKE or K3s) | Build, test, deploy; blue/green & canary releases; secret management. |

---

### Notes & Recommendations
* **Offline‑first**: local storage plus Firebase ensures uninterrupted field work.
* **Security**: all internal calls are mTLS; gateway enforces audience & scope claims.
* **Scalability**: services scale horizontally; message bus smooths burst traffic.
* **Extensibility**: additional analytics or ML services can subscribe to bus events without touching existing code.

> _This document captures the current reference architecture. Update it alongside infrastructure‑as‑code whenever components evolve._

