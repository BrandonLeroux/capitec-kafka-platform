#!/bin/bash
# Seed script: creates 1000 customers directly into the order-service DB.
# Customer numbers: 1000000000 → 1000000999
# Password for all: Capitec@01
# Re-runnable — uses upsert so safe to run multiple times.

set -euo pipefail

ORDER_SERVICE="http://localhost:8081"
COUNT=1000
START_NUM=1000000000

# SHA-256 of "Capitec@01"
PASSWORD_HASH=$(echo -n "Capitec@01" | sha256sum | awk '{print $1}')
echo "Password hash (Capitec@01): $PASSWORD_HASH"
echo ""

FIRST_NAMES=(
  "Thabo" "Sipho" "Nomsa" "Lerato" "Kabelo" "Zanele" "Mpho" "Siya" "Thandeka" "Bongani"
  "Ayanda" "Nompumelelo" "Lungelo" "Palesa" "Tshepo" "Lindiwe" "Sifiso" "Nokwanda" "Andile" "Dineo"
  "Nkosinathi" "Busisiwe" "Lethiwe" "Sibusiso" "Nandi" "Mbuso" "Khanyisile" "Sandile" "Phumzile" "Mlungisi"
  "Zinhle" "Musa" "Ntombizodwa" "Lungisa" "Simphiwe" "Yolanda" "Thulani" "Nozipho" "Sakhile" "Gugu"
  "James" "Sarah" "Michael" "Emma" "David" "Olivia" "Daniel" "Sophia" "Matthew" "Isabella"
  "Ethan" "Mia" "Alexander" "Amelia" "William" "Charlotte" "Benjamin" "Harper" "Elijah" "Evelyn"
)

LAST_NAMES=(
  "Dlamini" "Nkosi" "Zulu" "Mokoena" "Ndlovu" "Khumalo" "Mthembu" "Sithole" "Shabalala" "Mhlongo"
  "Mahlangu" "Buthelezi" "Xulu" "Cele" "Mkhize" "Majola" "Ntuli" "Mthethwa" "Ngubane" "Hadebe"
  "Zungu" "Vilakazi" "Ntanzi" "Gumbi" "Mchunu" "Bhengu" "Mbatha" "Ngcobo" "Msweli" "Ngema"
  "Smith" "Johnson" "Williams" "Brown" "Jones" "Miller" "Davis" "Wilson" "Taylor" "Anderson"
  "Thomas" "Jackson" "White" "Harris" "Martin" "Thompson" "Garcia" "Martinez" "Robinson" "Clark"
)

DOMAINS=("gmail.com" "outlook.com" "yahoo.com" "icloud.com" "proton.me" "capitec.co.za")
CELL_PREFIXES=("060" "061" "071" "072" "073" "074" "078" "079" "081" "082" "083" "084")

SUCCESS=0
FAILED=0

echo "Creating ${COUNT} customers (${START_NUM} → $((START_NUM + COUNT - 1)))..."
echo ""

for i in $(seq 0 $((COUNT - 1))); do
  CUST_NUM=$((START_NUM + i))

  FN=${FIRST_NAMES[$((i % ${#FIRST_NAMES[@]}))]}
  LN=${LAST_NAMES[$((i % ${#LAST_NAMES[@]}))]}
  DOMAIN=${DOMAINS[$((i % ${#DOMAINS[@]}))]}
  FN_LC=$(echo "$FN" | tr '[:upper:]' '[:lower:]')
  LN_LC=$(echo "$LN" | tr '[:upper:]' '[:lower:]')
  EMAIL="${FN_LC}.${LN_LC}@${DOMAIN}"
  PREFIX=${CELL_PREFIXES[$((i % ${#CELL_PREFIXES[@]}))]}
  CELL_SUFFIX=$(printf "%07d" $((( CUST_NUM * 7 + i * 13) % 9999999 + 1000000)))
  CELL="${PREFIX}${CELL_SUFFIX:0:7}"

  # Basic SA ID: YYMMDDSSSSXZZ
  YY=$(printf "%02d" $(( (i % 50) + 70 )))
  MM=$(printf "%02d" $(( (i % 12) + 1 )))
  DD=$(printf "%02d" $(( (i % 28) + 1 )))
  SEQ=$(printf "%04d" $(( (i % 9998) + 1 )))
  ID_NUMBER="${YY}${MM}${DD}${SEQ}088"

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${ORDER_SERVICE}/api/customer/register" \
    -H "Content-Type: application/json" \
    -d "{\"customerID\":\"${CUST_NUM}\",\"customerNumber\":${CUST_NUM},\"firstName\":\"${FN}\",\"lastName\":\"${LN}\",\"idNumber\":\"${ID_NUMBER}\",\"email\":\"${EMAIL}\",\"cell\":\"${CELL}\",\"passwordHash\":\"${PASSWORD_HASH}\"}")

  if [[ "$HTTP_CODE" == "200" ]]; then
    ((SUCCESS++))
  else
    ((FAILED++))
    echo "  [WARN] Failed customerNumber=${CUST_NUM} http=${HTTP_CODE}"
  fi

  if [[ $(( (i + 1) % 100 )) -eq 0 ]]; then
    echo "  $((i + 1)) / ${COUNT}  success=${SUCCESS}  failed=${FAILED}"
  fi
done

echo ""
echo "Done. Total=${COUNT}  Success=${SUCCESS}  Failed=${FAILED}"
echo ""
echo "Sample login:"
echo "  Cell:     $(echo "${CELL_PREFIXES[0]}0000000" | head -c 10)"
echo "  Password: Capitec@01"
echo ""
echo "To see first customer: curl -s '${ORDER_SERVICE}/api/customer/${START_NUM}' | python3 -m json.tool"
