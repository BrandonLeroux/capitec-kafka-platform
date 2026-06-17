#!/bin/bash
# Sends 1000 mixed messages to order-created via the producer UI.
# Mix: 70% valid orders, 15% faulty (non-JSON), 15% malformed JSON

ENDPOINT="http://localhost:8080/send"
TOTAL=1000
SUCCESS=0
FAILED=0

statuses=("PENDING" "CONFIRMED" "SHIPPED" "DELIVERED" "CANCELLED")
products=("SAVINGS_ACCOUNT" "PERSONAL_LOAN" "HOME_LOAN" "CREDIT_CARD" "VEHICLE_FINANCE")

send() {
  local key="$1"
  local value="$2"
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    --data-binary "{\"key\":\"$key\",\"value\":$(echo "$value" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}")
  if [[ "$http_code" == "200" ]]; then
    ((SUCCESS++))
  else
    ((FAILED++))
    echo "  [WARN] send failed key=$key http=$http_code"
  fi
}

echo "Sending $TOTAL messages to $ENDPOINT..."
echo ""

for i in $(seq 1 $TOTAL); do
  rand=$((i % 20))   # 0-19 to bucket message types

  # ── 70% valid orders (rand 0-13) ─────────────────────────────────────────
  if [[ $rand -le 13 ]]; then
    order_id="ORD-$(printf '%05d' $i)"
    amount=$(awk "BEGIN{printf \"%.2f\", ($RANDOM % 50000 + 100) / 100}")
    status=${statuses[$((RANDOM % 5))]}
    product=${products[$((RANDOM % 5))]}
    customer_id="CUST-$(printf '%04d' $((RANDOM % 9999 + 1)))"

    value="{\"orderID\":\"$order_id\",\"customerID\":\"$customer_id\",\"product\":\"$product\",\"amount\":$amount,\"status\":\"$status\"}"
    send "$order_id" "$value"

  # ── 15% faulty — plain string, not JSON (rand 14-16) ─────────────────────
  elif [[ $rand -le 16 ]]; then
    key="FAULTY-$(printf '%05d' $i)"
    case $((i % 3)) in
      0) value="plain text message number $i" ;;
      1) value="null" ;;
      2) value="12345" ;;
    esac
    send "$key" "$value"

  # ── 15% malformed JSON (rand 17-19) ──────────────────────────────────────
  else
    key="MALFORMED-$(printf '%05d' $i)"
    case $((i % 3)) in
      0) value="{orderID: missing-quotes, amount: $i}" ;;
      1) value="{\"orderID\":\"$i\", \"amount\":}" ;;
      2) value="{\"orderID\":\"$i\"" ;;   # unclosed brace — still starts with { so processor gets it
    esac
    send "$key" "$value"
  fi

  # Progress every 100
  if [[ $((i % 100)) -eq 0 ]]; then
    echo "  $i / $TOTAL sent (success=$SUCCESS failed=$FAILED)"
  fi
done

echo ""
echo "Done. Total=$TOTAL  Success=$SUCCESS  Failed=$FAILED"
