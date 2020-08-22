<?php

namespace yii\queue\redis;

class Scripts
{
    const STATUS = <<<LUA
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
    return 2
end

if redis.call("HEXISTS", KEYS[2], ARGV[1]) == 1 then
    return 1
end

return 3
LUA;

    const CLEAR = <<<LUA
local keys = redis.call('KEYS', ARGV[1])

for i=1, #keys, 5000 do
    redis.call('DEL', unpack(keys, i, math.min(i + 4999, #keys)))
end

return keys
LUA;

    const REMOVE = <<<LUA
local result = redis.call("HDEL", KEYS[1], ARGV[1])

redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("LREM", KEYS[4], 0, ARGV[1])
redis.call("HDEL", KEYS[5], ARGV[1])

return result
LUA;

    const MOVE_EXPIRED = <<<LUA
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])

for i=1, #ids, 5000 do
    redis.call("LPUSH", KEYS[2], unpack(ids, i, math.min(i + 4999, #ids)))
end

redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
LUA;

    const DELETE = <<<LUA
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
LUA;

    const PUSH = <<<LUA
local id = redis.call("INCR", KEYS[1])

redis.call("HSET", KEYS[2], id, ARGV[1])

if tonumber(ARGV[2]) == 0 then
    redis.call("LPUSH", KEYS[3], id)
else
    redis.call("ZADD", KEYS[4], ARGV[3] + ARGV[2], id)
end

return id
LUA;

    const RESERVE = <<<LUA
local id = redis.call("RPOP", KEYS[1])
if id == false then
    return nil
end

local payload = redis.call("HGET", KEYS[2], id)
if payload == false then
    return nil
end

local idx = string.find(payload, ";")
local ttr = string.sub(payload, 0, idx - 1)
local message = string.sub(payload, idx + 1)

redis.call("ZADD", KEYS[3], ARGV[1] + ttr, id)
local attempt = redis.call("HINCRBY", KEYS[4], id, 1)

return {id, message, ttr, attempt}
LUA;
}
