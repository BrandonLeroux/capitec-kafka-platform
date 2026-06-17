#!/bin/bash
# Stocks up the inventory DB by publishing to the inventory Kafka topic.
# Key = SKU (inventory code), Action = SET (full restock).
# Re-runnable — overwrites existing quantities.
# Usage: bash stock-inventory.sh [quantity-per-item]  (default 100)

set -euo pipefail

INVENTORY_SERVICE="http://localhost:8083"
DEFAULT_QTY=${1:-100}

declare -a ITEMS=(
  # SKU|productID|name|category|qty|reorderLevel|price
  "TYR-175-65-R14|TYRE_175_65_R14|175/65 R14 Economy|Tyres|${DEFAULT_QTY}|15|849.00"
  "TYR-195-65-R15|TYRE_195_65_R15|195/65 R15 Standard|Tyres|${DEFAULT_QTY}|15|1149.00"
  "TYR-205-55-R16|TYRE_205_55_R16|205/55 R16 Performance|Tyres|${DEFAULT_QTY}|10|1549.00"
  "TYR-235-65-R17|TYRE_235_65_R17|235/65 R17 SUV All-Season|Tyres|${DEFAULT_QTY}|10|2199.00"
  "TYR-265-70-R17|TYRE_265_70_R17|265/70 R17 4x4 Off-Road|Tyres|${DEFAULT_QTY}|8|3299.00"
  "TYR-RF-205-17|TYRE_RUN_FLAT_17|205/45 R17 Run-Flat|Tyres|${DEFAULT_QTY}|8|2899.00"
  "BRK-PAD-FS|BRAKE_PAD_FRONT_STD|Front Brake Pads Standard|Brakes|${DEFAULT_QTY}|20|649.00"
  "BRK-PAD-FP|BRAKE_PAD_FRONT_PERF|Front Brake Pads Performance|Brakes|${DEFAULT_QTY}|15|1199.00"
  "BRK-PAD-RS|BRAKE_PAD_REAR_STD|Rear Brake Pads Standard|Brakes|${DEFAULT_QTY}|20|549.00"
  "BRK-DSC-F|BRAKE_DISC_FRONT|Front Brake Disc Vented|Brakes|${DEFAULT_QTY}|15|1350.00"
  "BRK-DSC-R|BRAKE_DISC_REAR|Rear Brake Disc Solid|Brakes|${DEFAULT_QTY}|15|980.00"
  "BRK-KIT-FULL|BRAKE_KIT_FULL|Complete Brake Kit Front+Rear|Brakes|${DEFAULT_QTY}|10|4499.00"
  "BAT-NS40|BATT_NS40|NS40 35Ah Compact|Batteries|${DEFAULT_QTY}|20|899.00"
  "BAT-N60|BATT_N60|N60 60Ah Standard|Batteries|${DEFAULT_QTY}|20|1299.00"
  "BAT-N70|BATT_N70|N70 70Ah Heavy Duty|Batteries|${DEFAULT_QTY}|15|1699.00"
  "BAT-AGM-70|BATT_AGM_70|AGM 70Ah Start-Stop|Batteries|${DEFAULT_QTY}|10|2799.00"
  "BAT-AGM-95|BATT_AGM_95|AGM 95Ah Premium|Batteries|${DEFAULT_QTY}|8|3499.00"
  "FLT-OIL-S|FILTER_OIL_STD|Oil Filter Standard|Filters|${DEFAULT_QTY}|30|129.00"
  "FLT-OIL-P|FILTER_OIL_PREM|Oil Filter Premium Synthetic|Filters|${DEFAULT_QTY}|20|249.00"
  "FLT-AIR-PNL|FILTER_AIR_PANEL|Panel Air Filter|Filters|${DEFAULT_QTY}|25|299.00"
  "FLT-AIR-PRF|FILTER_AIR_PERF|High-Flow Performance Air Filter|Filters|${DEFAULT_QTY}|15|799.00"
  "FLT-FUEL|FILTER_FUEL|Inline Fuel Filter|Filters|${DEFAULT_QTY}|25|199.00"
  "FLT-CAB|FILTER_CABIN|Cabin Pollen Dust Filter|Filters|${DEFAULT_QTY}|25|259.00"
  "WPR-FL-600|WIPER_FLAT_600|Flat Blade 600mm Driver|Wipers|${DEFAULT_QTY}|20|229.00"
  "WPR-FL-400|WIPER_FLAT_400|Flat Blade 400mm Passenger|Wipers|${DEFAULT_QTY}|20|199.00"
  "WPR-CV-PR|WIPER_CONV_PAIR|Conventional Pair Front|Wipers|${DEFAULT_QTY}|20|299.00"
  "WPR-REAR|WIPER_REAR|Rear Wiper Blade 300mm|Wipers|${DEFAULT_QTY}|15|149.00"
  "WPR-AS-PAIR|WIPER_ALL_SEASON|All-Season Hybrid Pair|Wipers|${DEFAULT_QTY}|15|549.00"
  "SHK-FR-GAS|SHOCK_FRONT_GAS|Front Gas Shock Standard|Shocks|${DEFAULT_QTY}|12|899.00"
  "SHK-RR-GAS|SHOCK_REAR_GAS|Rear Gas Shock Standard|Shocks|${DEFAULT_QTY}|12|799.00"
  "SHK-FR-SPT|SHOCK_FRONT_SPORT|Front Sport Shock Lowered|Shocks|${DEFAULT_QTY}|8|1499.00"
  "SHK-RR-SPT|SHOCK_REAR_SPORT|Rear Sport Shock Lowered|Shocks|${DEFAULT_QTY}|8|1299.00"
  "SHK-KIT-4|SHOCK_KIT_FULL|Full Set 4 Gas Shocks|Shocks|${DEFAULT_QTY}|8|2999.00"
  "SHK-BK-HD|SHOCK_BAKKIE_HEAVY|Heavy Duty Bakkie Shock|Shocks|${DEFAULT_QTY}|10|1799.00"
  "LGT-H4-HAL|BULB_H4_HALOGEN|H4 Halogen Bulb Twin Pack|Lighting|${DEFAULT_QTY}|25|179.00"
  "LGT-H7-HAL|BULB_H7_HALOGEN|H7 Halogen Bulb Twin Pack|Lighting|${DEFAULT_QTY}|25|229.00"
  "LGT-H4-XEN|BULB_H4_XENON|H4 Xenon-White Bulb Twin|Lighting|${DEFAULT_QTY}|20|399.00"
  "LGT-LED-H7|BULB_LED_H7|LED H7 Conversion Twin|Lighting|${DEFAULT_QTY}|15|1299.00"
  "LGT-LED-BAR|LIGHT_LED_BAR|LED Light Bar 120W 20inch|Lighting|${DEFAULT_QTY}|8|1999.00"
  "LGT-REV-LED|LIGHT_REVERSE_LED|LED Reverse Light Pair|Lighting|${DEFAULT_QTY}|20|499.00"
  "OIL-5W30-1|OIL_5W30_1L|5W-30 Full Synthetic 1L|Oils|${DEFAULT_QTY}|30|219.00"
  "OIL-5W30-5|OIL_5W30_5L|5W-30 Full Synthetic 5L|Oils|${DEFAULT_QTY}|25|899.00"
  "OIL-10W40-5|OIL_10W40_5L|10W-40 Semi-Synthetic 5L|Oils|${DEFAULT_QTY}|25|649.00"
  "OIL-15W40-5|OIL_15W40_5L|15W-40 Mineral 5L|Oils|${DEFAULT_QTY}|20|399.00"
  "OIL-0W20-5|OIL_0W20_5L|0W-20 Full Synthetic 5L|Oils|${DEFAULT_QTY}|15|1099.00"
  "OIL-DSL-5W40|OIL_DIESEL_5L|5W-40 Diesel Synthetic 5L|Oils|${DEFAULT_QTY}|20|979.00"
)

SUCCESS=0
FAILED=0
TOTAL=${#ITEMS[@]}

echo "Stocking ${TOTAL} inventory items at qty=${DEFAULT_QTY} each..."
echo ""

for ITEM in "${ITEMS[@]}"; do
  IFS='|' read -r SKU PRODUCT_ID NAME CATEGORY QTY REORDER PRICE <<< "$ITEM"

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${INVENTORY_SERVICE}/api/inventory/seed" \
    -H "Content-Type: application/json" \
    -d "{\"sku\":\"${SKU}\",\"productID\":\"${PRODUCT_ID}\",\"name\":\"${NAME}\",\"category\":\"${CATEGORY}\",\"quantity\":${QTY},\"reorderLevel\":${REORDER},\"unitPrice\":${PRICE}}")

  if [[ "$HTTP_CODE" == "200" ]]; then
    ((SUCCESS++))
  else
    ((FAILED++))
    echo "  [WARN] Failed sku=${SKU} http=${HTTP_CODE}"
  fi
done

echo "Done. Total=${TOTAL}  Success=${SUCCESS}  Failed=${FAILED}"
echo ""
echo "Inventory service will consume the events and update the DB."
echo "Check stock levels at http://localhost:8083/api/inventory"
