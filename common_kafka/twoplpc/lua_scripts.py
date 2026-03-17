"""
Lua scripts for atomic 2PL/2PC lock acquisition with idempotency.

These scripts ensure crash recovery works correctly by:
1. Storing the lock_id in a transaction-scoped key (tx_key)
2. On replay, returning the existing lock_id instead of creating a new one
3. Preventing orphaned locks from message replays after crashes
"""

ACQUIRE_AND_PREPARE_PAYMENT_LUA = """
-- KEYS[1] = lock_key   (payment:2pc:userlock:{user_id})
-- KEYS[2] = tx_key     (payment:2pc:tx:{order_id})
-- KEYS[3] = prep_key   (payment:2pc:lock:{lock_id})
-- ARGV[1] = transaction_id
-- ARGV[2] = ttl
-- ARGV[3] = transaction_id (for hash field)
-- ARGV[4] = user_id
-- ARGV[5] = amount
-- ARGV[6] = lock_id

local lock_key = KEYS[1]
local tx_key   = KEYS[2]
local prep_key = KEYS[3]
local tx_id    = ARGV[1]
local ttl      = tonumber(ARGV[2])
local user_id  = ARGV[4]
local amount   = ARGV[5]
local lock_id  = ARGV[6]

-- 1. Idempotency check: already processed this transaction?
local existing_lock_id = redis.call('GET', tx_key)
if existing_lock_id then
    return {1, existing_lock_id}
end

-- 2. Check if user locked by different transaction
local holder = redis.call('GET', lock_key)
if holder and holder ~= tx_id then
    return {0, lock_key}
end

-- 3. Acquire: set userlock, tx->lock_id mapping, and prepared record
redis.call('SET', lock_key, tx_id, 'EX', ttl)
redis.call('SET', tx_key, lock_id, 'EX', ttl)
redis.call('HSET', prep_key,
    'transaction_id', tx_id,
    'user_id',        user_id,
    'amount',         amount,
    'status',         'prepared'
)
redis.call('EXPIRE', prep_key, ttl)

return {1, lock_id}
"""

ACQUIRE_AND_PREPARE_STOCK_LUA = """
-- KEYS[1..n-2] = lock_keys (stock:2pc:itemlock:{item_id} for each item)
-- KEYS[n-1]    = tx_key    (stock:2pc:tx:{order_id})
-- KEYS[n]      = prep_key  (stock:2pc:lock:{lock_id})
-- ARGV[1] = transaction_id
-- ARGV[2] = ttl
-- ARGV[3] = transaction_id (for hash field)
-- ARGV[4] = items (msgpack bytes)
-- ARGV[5] = lock_id

local num_keys = #KEYS
local tx_key   = KEYS[num_keys - 1]
local prep_key = KEYS[num_keys]
local tx_id    = ARGV[1]
local ttl      = tonumber(ARGV[2])
local items    = ARGV[4]
local lock_id  = ARGV[5]

-- 1. Idempotency check: already processed this transaction?
local existing_lock_id = redis.call('GET', tx_key)
if existing_lock_id then
    return {1, existing_lock_id}
end

-- 2. Check all item locks - fail if any held by different transaction
for i = 1, num_keys - 2 do
    local holder = redis.call('GET', KEYS[i])
    if holder and holder ~= tx_id then
        return {0, KEYS[i]}
    end
end

-- 3. Acquire all item locks + tx->lock_id mapping + prepared record
for i = 1, num_keys - 2 do
    redis.call('SET', KEYS[i], tx_id, 'EX', ttl)
end
redis.call('SET', tx_key, lock_id, 'EX', ttl)
redis.call('HSET', prep_key,
    'transaction_id', tx_id,
    'items',          items,
    'status',         'prepared'
)
redis.call('EXPIRE', prep_key, ttl)

return {1, lock_id}
"""
