ACQUIRE_AND_PREPARE_STOCK_LUA = """
local tx_id   = ARGV[1]
local ttl     = tonumber(ARGV[2])
local prep_key = KEYS[#KEYS]

for i = 1, #KEYS - 1 do
    if redis.call('EXISTS', KEYS[i]) == 1 then
        return {0, KEYS[i]}
    end
end

for i = 1, #KEYS - 1 do
    redis.call('SET', KEYS[i], tx_id, 'EX', ttl)
end

redis.call('HSET', prep_key,
    'transaction_id', ARGV[3],
    'items',          ARGV[4],
    'status',         'prepared'
)
redis.call('EXPIRE', prep_key, ttl)

return {1, ''}
"""

ACQUIRE_AND_PREPARE_PAYMENT_LUA = """
local tx_id   = ARGV[1]
local ttl     = tonumber(ARGV[2])
local prep_key = KEYS[2]           -- only one lock key + one prepared key

if redis.call('EXISTS', KEYS[1]) == 1 then
    return {0, KEYS[1]}
end

redis.call('SET', KEYS[1], tx_id, 'EX', ttl)

redis.call('HSET', prep_key,
    'transaction_id', ARGV[3],
    'user_id',        ARGV[4],
    'amount',         ARGV[5],
    'status',         'prepared'
)
redis.call('EXPIRE', prep_key, ttl)

return {1, ''}
"""
