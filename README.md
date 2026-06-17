# Capitec Kafka Platform

A full event-driven microservices platform built on Apache Kafka, deployed on Kubernetes (Rancher Desktop).

## Platform Compatibility

| Component | macOS | Windows (WSL) |
|---|---|---|
| Java services | ✅ | ✅ |
| Docker / Kubernetes | ✅ Rancher Desktop | ✅ Rancher Desktop |
| Shell scripts (`runbook.sh`, seed scripts) | ✅ | ✅ via WSL |

**Windows users must run all scripts inside WSL.** Open a WSL terminal and clone the repo there — do not run the scripts from CMD or PowerShell.

## Architecture

```
customer-portal  :8082   Customer-facing order portal (login, shop, cart, cancellation)
kafka-order-service :8081 Admin dashboard + order/customer/inventory management
inventory-service :8083  Inventory tracking and stock management
payment-processor        Mock payment fulfilment pipeline
kafka-producer           Dev tool — raw message publishing UI
kafka-consumer           Standalone consumer (DLT + retry handling)
```

## Kafka Topics

| Topic | Purpose |
|---|---|
| `customer-created` | New customer registrations |
| `order-created` | All order lifecycle events (new + status updates) |
| `order-cancelled` | Cancellation requests with reason |
| `payment-init` | Triggers payment processor |
| `inventory` | Inventory SET and ADJUST events (key = SKU) |

## Order Status Lifecycle

```
CONFIRMED → PAYMENT-INIT → PAYMENT-PROCESSED → PACKED → OUT-FOR-DELIVERY → DELIVERED
                                                     ↓
                                                 CANCELLED (with reason)
```

## Services

### customer-portal
- Login / register (cell + password, SHA-256 hashed)
- Customer numbers auto-assigned from 1000000000
- Shop: 8 car-parts categories, 46 SKUs with live stock levels
- Cart with qty selector (dropdown + free-text)
- Checkout places orders on `order-created` and deducts inventory
- Cancel orders (CONFIRMED → PACKED) with reason → `order-cancelled` + inventory restore
- My Orders with status timeline

### kafka-order-service (admin dashboard)
- Orders tab: searchable, filterable, cancellation reason column
- Customers tab: customer number, contact details
- Inventory tab: SKU, stock levels, reorder alerts
- Consumes: `order-created`, `customer-created`, `order-cancelled`
- Produces to: `payment-init`

### inventory-service
- Consumes `inventory` topic (key = SKU)
- `SET` action: full restock to quantity
- `ADJUST` action: signed delta (negative = deduct, positive = restore)
- REST: `GET /api/inventory`, `GET /api/inventory/stock`, `POST /api/inventory/seed`

### payment-processor
- Consumes `payment-init`
- Mocks fulfilment pipeline with delays:
  - 2s → PAYMENT-PROCESSED
  - 3s → PACKED
  - 3s → OUT-FOR-DELIVERY
  - 5s → DELIVERED
- All status updates published back to `order-created`

## Seed Scripts

```bash
# Seed 1000 customers (password: Capitec@01, numbers 1000000000–1000000999)
bash kafka-producer/create-customers.sh

# Seed ~2000 orders spread across all status stages
bash kafka-producer/create-orders.sh

# Stock all 46 inventory SKUs at 100 units each (or custom qty)
bash kafka-producer/stock-inventory.sh [qty]
```

## Kubernetes Deployment

All manifests are in `k8s/`. Built for local Rancher Desktop (`imagePullPolicy: Never`).

```bash
# Build and deploy a service
cd <service-dir>
mvn clean package -q
docker build -t <image-name>:latest .
kubectl apply -f k8s/<service>.yaml
```

## Tech Stack

- **Java 17** — all services, no Spring, plain `ServerSocket` HTTP
- **Apache Kafka 3.7** — event streaming
- **SQLite** — embedded DB (WAL mode, synchronized access)
- **Rancher Desktop** — local Kubernetes (k3s)
- **Eclipse Temurin 21** — Docker base image
