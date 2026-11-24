package rmq

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisWrapper struct {
	rawClient redis.Cmdable
}

func (wrapper RedisWrapper) Set(key string, value string, expiration time.Duration) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.Set(context.TODO(), key, value, expiration).Err()
}

func (wrapper RedisWrapper) Del(key string) (affected int64, err error) {
	return wrapper.rawClient.Del(context.TODO(), key).Result()
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, err error) {
	return wrapper.rawClient.TTL(context.TODO(), key).Result()
}

func (wrapper RedisWrapper) Exists(key string) (int64, error) {
	return wrapper.rawClient.Exists(context.TODO(), key).Result()
}

func (wrapper RedisWrapper) ZAdd(key string, member string, score float64) error {
	return wrapper.rawClient.ZAdd(context.TODO(), key, redis.Z{Score: score, Member: member}).Err()
}

func (wrapper RedisWrapper) ZRange(key string, start int64, stop int64) ([]string, error) {
	return wrapper.rawClient.ZRange(context.TODO(), key, start, stop).Result()
}

func (wrapper RedisWrapper) ZRangeByScore(key string, min float64, max float64) ([]string, error) {
	return wrapper.rawClient.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{
		Min: strconv.FormatFloat(min, 'f', -1, 64),
		Max: strconv.FormatFloat(max, 'f', -1, 64),
	}).Result()
}

func (wrapper RedisWrapper) ZRem(key string, member string) (int64, error) {
	return wrapper.rawClient.ZRem(context.TODO(), key, member).Result()
}

func (wrapper RedisWrapper) LPush(key string, value ...string) (total int64, err error) {
	return wrapper.rawClient.LPush(context.TODO(), key, value).Result()
}

func (wrapper RedisWrapper) LLen(key string) (affected int64, err error) {
	return wrapper.rawClient.LLen(context.TODO(), key).Result()
}

func (wrapper RedisWrapper) LRem(key string, count int64, value string) (affected int64, err error) {
	return wrapper.rawClient.LRem(context.TODO(), key, int64(count), value).Result()
}

func (wrapper RedisWrapper) LTrim(key string, start, stop int64) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.LTrim(context.TODO(), key, int64(start), int64(stop)).Err()
}

func (wrapper RedisWrapper) RPop(key string) (value string, err error) {
	return wrapper.rawClient.RPop(context.TODO(), key).Result()
}

func (wrapper RedisWrapper) RPopLPush(source, destination string) (value string, err error) {
	value, err = wrapper.rawClient.RPopLPush(context.TODO(), source, destination).Result()
	// println("RPopLPush", source, destination, value, err)
	switch err {
	case nil:
		return value, nil
	case redis.Nil:
		return value, ErrorNotFound
	default:
		return value, err
	}
}

func (wrapper RedisWrapper) SAdd(key, value string) (total int64, err error) {
	return wrapper.rawClient.SAdd(context.TODO(), key, value).Result()
}

func (wrapper RedisWrapper) SMembers(key string) (members []string, err error) {
	return wrapper.rawClient.SMembers(context.TODO(), key).Result()
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int64, err error) {
	return wrapper.rawClient.SRem(context.TODO(), key, value).Result()
}

func (wrapper RedisWrapper) FlushDb() error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.FlushDB(context.TODO()).Err()
}
