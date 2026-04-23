package store

import "time"

type entry struct {
	value     string
	expiresAt time.Time
}

func (e entry) isExpired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}