<?php

namespace yii\queue\redis;

class Scripts
{
    const STATUS = <<<LUA
local attemptsKey, messagesKey = unpack(KEYS)
local id, waitingStatus, reservedStatus, doneStatus = unpack(ARGV)

if redis.call("HEXISTS", attemptsKey, id) == 1 then
    return reservedStatus
end

if redis.call("HEXISTS", messagesKey, id) == 1 then
    return waitingStatus
end

return doneStatus
LUA;

    const CLEAR = <<<LUA
local wildcard = unpack(ARGV)
local keys = redis.call('KEYS', wildcard)

for i=1, #keys, 5000 do
    redis.call('DEL', unpack(keys, i, math.min(i + 4999, #keys)))
end

return keys
LUA;

    const REMOVE = <<<LUA
local messagesKey, delayedKey, reservedKey, waitingKey, attemptsKey = unpack(KEYS)
local id = unpack(ARGV)

local result = redis.call("HDEL", messagesKey, id)

redis.call("ZREM", delayedKey, id)
redis.call("ZREM", reservedKey, id)
redis.call("LREM", waitingKey, 0, id)
redis.call("HDEL", attemptsKey, id)

return result
LUA;

    const MOVE_EXPIRED = <<<LUA
local fromSet, toList = unpack(KEYS)
local threshold = unpack(ARGV)
local ids = redis.call("ZRANGEBYSCORE", fromSet, "-inf", threshold)

for i=1, #ids, 5000 do
    redis.call("LPUSH", toList, unpack(ids, i, math.min(i + 4999, #ids)))
end

redis.call("ZREMRANGEBYSCORE", fromSet, "-inf", threshold)
LUA;

    const DELETE = <<<LUA
local reservedKey, attemptsKey, messagesKey = unpack(KEYS)
local id = unpack(ARGV)

redis.call("ZREM", reservedKey, id)
redis.call("HDEL", attemptsKey, id)
redis.call("HDEL", messagesKey, id)
LUA;

    const PUSH = <<<LUA
local messageIdKey, messagesKey, waitingKey, delayedKey = unpack(KEYS)
local payload, delay, now = unpack(ARGV)

local id = redis.call("INCR", messageIdKey)

redis.call("HSET", messagesKey, id, payload)

if tonumber(delay) == 0 then
    redis.call("LPUSH", waitingKey, id)
else
    redis.call("ZADD", delayedKey, now + delay, id)
end

return id
LUA;

    const RESERVE = <<<LUA
local waitingKey, messagesKey, reservedKey, attemptsKey = unpack(KEYS)
local now = unpack(ARGV)

local id = redis.call("RPOP", waitingKey)
if id == false then
    return nil
end

local payload = redis.call("HGET", messagesKey, id)
if payload == false then
    return nil
end

local idx = string.find(payload, ";")
local ttr = string.sub(payload, 0, idx - 1)
local message = string.sub(payload, idx + 1)

redis.call("ZADD", reservedKey, now + ttr, id)
local attempt = redis.call("HINCRBY", attemptsKey, id, 1)

return {id, message, ttr, attempt}
LUA;
}
