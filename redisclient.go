package redis_lock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"github.com/go-redis/redis/v8"
	"io"
	"sync"
	"time"
)

type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, script ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

type Client struct {
	client RedisClient
	tmp    []byte
	tmpMu  sync.Mutex
}

func New(client RedisClient) *Client {
	return &Client{client: client}
}

func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	token, err := c.randomToken()
	if err != nil {
		return nil, err
	}
	val := token + opt.getMetadata()
	retry := opt.getRetryStrategy()
	deadlineCtx, cancel := context.WithDeadline(ctx, time.Now().Add(ttl))
	var timer *time.Timer
	defer cancel()
	for {
		ok, err := c.obtain(ctx, key, val, ttl)
		if err != nil {
			return nil, err
		} else if ok {
			return &Lock{client: c, key: key, val: val}, nil
		}
		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}
		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}
		select {
		case <-deadlineCtx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

func (c *Client) obtain(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, val, ttl).Result()
}

func (c *Client) randomToken() (string, error) {
	c.tmpMu.Lock()
	defer c.tmpMu.Unlock()
	if len(c.tmp) == 0 {
		c.tmp = make([]byte, 16)
	}
	if _, err := io.ReadFull(rand.Reader, c.tmp); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(c.tmp), nil
}
