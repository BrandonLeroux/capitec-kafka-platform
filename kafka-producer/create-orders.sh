#!/bin/bash
# Seed script: generates orders across ALL status stages for dashboard display.
# Uses order-service REST API directly (bypasses Kafka so statuses stay fixed).
# Re-runnable — order IDs include a timestamp so each run creates fresh orders.
#
# Status spread per 1000 customers (~2000 orders):
#   CONFIRMED        15%  — just placed, awaiting payment trigger
#   PAYMENT-INIT     15%  — payment initiated
#   PAYMENT-PROCESSED 15% — payment confirmed
#   PACKED           15%  — being packed
#   OUT-FOR-DELIVERY 20%  — on the way
#   DELIVERED        20%  — completed

set -euo pipefail

ORDER_SERVICE="http://localhost:8081"
START_NUM=1000000000
CUST_COUNT=1000

PRODUCTS=(
  "TYRE_175_65_R14" "TYRE_195_65_R15" "TYRE_205_55_R16" "TYRE_235_65_R17" "TYRE_265_70_R17"
  "BRAKE_PAD_FRONT_STD" "BRAKE_PAD_FRONT_PERF" "BRAKE_DISC_FRONT" "BRAKE_KIT_FULL"
  "BATT_NS40" "BATT_N60" "BATT_N70" "BATT_AGM_70"
  "FILTER_OIL_STD" "FILTER_AIR_PANEL" "FILTER_CABIN"
  "WIPER_FLAT_600" "WIPER_CONV_PAIR" "WIPER_ALL_SEASON"
  "SHOCK_FRONT_GAS" "SHOCK_REAR_GAS" "SHOCK_KIT_FULL"
  "BULB_H4_HALOGEN" "BULB_LED_H7" "LIGHT_LED_BAR"
  "OIL_5W30_5L" "OIL_10W40_5L" "OIL_DIESEL_5L"
)

PRICES=(
   849  1149  1549  2199  3299
   649  1199  1350  4499
   899  1299  1699  2799
   129   299   259
   229   299   549
   899   799  2999
   179  1299  1999
   899   649   979
)

STATUSES=("CONFIRMED" "CONFIRMED" "CONFIRMED"
          "PAYMENT-INIT" "PAYMENT-INIT" "PAYMENT-INIT"
          "PAYMENT-PROCESSED" "PAYMENT-PROCESSED" "PAYMENT-PROCESSED"
          "PACKED" "PACKED" "PACKED"
          "OUT-FOR-DELIVERY" "OUT-FOR-DELIVERY" "OUT-FOR-DELIVERY" "OUT-FOR-DELIVERY"
          "DELIVERED" "DELIVERED" "DELIVERED" "DELIVERED")

TOTAL_ORDERS=0
SUCCESS=0
FAILED=0
TS=$(date +%s)

echo "Generating orders for ${CUST_COUNT} customers across all status stages..."
echo ""

for i in $(seq 0 $((CUST_COUNT - 1))); do
  CUST_NUM=$((START_NUM + i))

  # 1-3 orders per customer
  NUM_ORDERS=$(( (i % 3) + 1 ))

  for j in $(seq 1 ${NUM_ORDERS}); do
    P_IDX=$((( CUST_NUM + j * 7 ) % ${#PRODUCTS[@]}))
    PRODUCT=${PRODUCTS[$P_IDX]}
    PRICE=${PRICES[$P_IDX]}
    QTY=$(( (i % 3) + 1 ))
    AMOUNT=$(( PRICE * QTY ))

    S_IDX=$((( i * 3 + j * 7 ) % ${#STATUSES[@]}))
    STATUS=${STATUSES[$S_IDX]}

    ORDER_ID="ORD-${CUST_NUM}-${TS}-${j}"

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST "${ORDER_SERVICE}/api/order/seed" \
      -H "Content-Type: application/json" \
      -d "{\"orderID\":\"${ORDER_ID}\",\"customerID\":\"${CUST_NUM}\",\"product\":\"${PRODUCT}\",\"amount\":${AMOUNT}.00,\"status\":\"${STATUS}\"}")

    ((TOTAL_ORDERS++))
    if [[ "$HTTP_CODE" == "200" ]]; then
      ((SUCCESS++))
    else
      ((FAILED++))
      [[ $FAILED -le 5 ]] && echo "  [WARN] Failed orderID=${ORDER_ID} status=${STATUS} http=${HTTP_CODE}"
    fi
  done

  if [[ $(( (i + 1) % 100 )) -eq 0 ]]; then
    echo "  $((i + 1)) / ${CUST_COUNT}  orders=${TOTAL_ORDERS}  success=${SUCCESS}  failed=${FAILED}"
  fi
done

echo ""
echo "Done."
echo "  Total orders : ${TOTAL_ORDERS}"
echo "  Success      : ${SUCCESS}"
echo "  Failed       : ${FAILED}"
echo ""
echo "Check the dashboard at ${ORDER_SERVICE}"
