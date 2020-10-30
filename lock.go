package redis_lock

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
)

const (
	luaRefreshStr = `
	if redis.call("get",KEYS[1]) == ARGV[1]
	then
		return redis.call("pexpire",KEYS[1],ARGV[2])
	else
		return 0
	end
	`
	luaReleaseStr = `
	if redis.call("get",KYES[1]) == ARGV[1]
	then
		return redis.call("del",KEYS[1])
	else 
		return 0
	end
	`
	luaPTTLStr = `
	if redis.call("get",KEYS[1]) == ARGV[1]
	then
		redis.call("pttl",KEYS[1])
	else
		return -3
	end
	`
)

var (
	luaRefresh = redis.NewScript(luaRefreshStr)
	luaRelease = redis.NewScript(luaReleaseStr)
	luaPTTL    = redis.NewScript(luaPTTLStr)
)

type Lock struct {
	client *Client
	key    string
	val    string
}

func (l *Lock) Key() string {
	return l.key
}

func (l *Lock) Token() string {
	return l.val[:16]
}
func (l *Lock) Metadata() string {
	return l.val[16:]
}
func (l *Lock) TLL(ctx context.Context) (time.Duration, error) {
	res, err := luaPTTL.Run(ctx, l.client.client, []string{l.key}, l.val).Result()
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	if num := res.(int64); num > 0 {
		return time.Duration(num) * time.Millisecond, nil
	}
	return 0, nil
}
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaRefresh.Run(ctx, l.client.client, []string{l.key}, l.val, ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}
func (l *Lock) Release(ctx context.Context) error {
	res, err := luaRelease.Run(ctx, l.client.client, []string{l.key}, l.val).Result()
	if err == redis.Nil {
		return ErrLockNotHeld
	} else if err != nil {
		return err
	}
	if i, ok := res.(int64); !ok || i != 1 {
		return ErrLockNotHeld
	}
	return nil
}
