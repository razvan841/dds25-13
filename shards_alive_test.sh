GATEWAY="${GATEWAY:-http://localhost:8000}"

set -euo pipefail

echo "Seeding 1 entity per shard (payment, stock, orders)..."
for N in 0 1 2; do
  curl -fsS -X POST "$GATEWAY/payment/shard/$N/batch_init/1/100" >/dev/null
  curl -fsS -X POST "$GATEWAY/stock/shard/$N/batch_init/1/10/5" >/dev/null
  curl -fsS -X POST "$GATEWAY/orders/shard/$N/batch_init/1/1/1/5" >/dev/null
done
echo "Seed done."

echo
echo "Verifying each order ID routes to a different shard-owned dataset:"
for ID in 0 1 2; do
  echo "---- /orders/find/$ID"
  curl -fsS "$GATEWAY/orders/find/$ID"; echo
done

echo
echo "Verifying matching payment users exist:"
for ID in 0 1 2; do
  echo "---- /payment/find_user/$ID"
  curl -fsS "$GATEWAY/payment/find_user/$ID"; echo
done

echo
echo "Verifying matching stock items exist:"
for ID in 0 1 2; do
  echo "---- /stock/find/$ID"
  curl -fsS "$GATEWAY/stock/find/$ID"; echo
done

echo
echo "If all 9 reads succeeded, shard routing for integer IDs is working."