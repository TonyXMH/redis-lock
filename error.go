package redis_lock

import "errors"

var (
	ErrNotObtained = errors.New("redislock: not obtained")
	ErrLockNotHeld = errors.New("redislock: lock not held")
)
