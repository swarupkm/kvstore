package store

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type WAL struct {
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Write appends an operation to the WAL.
// expireAt is a Unix nanosecond timestamp, 0 means no expiry.
func (w *WAL) Write(op, key, value string, expireAt int64) error {
	_, err := fmt.Fprintf(w.file, "%s\t%s\t%s\t%d\n", op, key, value, expireAt)
	return err
}

func (w *WAL) Replay(s *Store) error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 4)
		if len(parts) < 2 {
			continue
		}

		op := parts[0]
		key := parts[1]
		value := ""
		if len(parts) >= 3 {
			value = parts[2]
		}
		var expireAt int64
		if len(parts) == 4 {
			expireAt, _ = strconv.ParseInt(parts[3], 10, 64)
		}

		switch op {
		case "SET":
			if expireAt > 0 {
				ttl := time.Until(time.Unix(0, expireAt))
				if ttl > 0 {
					s.SetWithTTL(key, value, ttl)
				}
				// if ttl <= 0 the key already expired — skip it
			} else {
				s.Set(key, value)
			}
		case "DEL":
			s.Delete(key)
		case "INCR":
			s.Increment(key)
		}
	}
	return scanner.Err()
}

func (w *WAL) Compact(s *Store) error {
	s.mu.RLock()
	snapshot := make(map[string]entry, len(s.data))
	for k, v := range s.data {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	tmpPath := w.file.Name() + ".tmp"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(tmp)
	for k, e := range snapshot {
		if e.isExpired() {
			continue
		}
		var expireAt int64
		if !e.expiresAt.IsZero() {
			expireAt = e.expiresAt.UnixNano()
		}
		if _, err := fmt.Fprintf(writer, "SET\t%s\t%s\t%d\n", k, e.value, expireAt); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	tmp.Close()

	if err := os.Rename(tmpPath, w.file.Name()); err != nil {
		os.Remove(tmpPath)
		return err
	}

	newFile, err := os.OpenFile(w.file.Name(), os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	w.file.Close()
	w.file = newFile
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}