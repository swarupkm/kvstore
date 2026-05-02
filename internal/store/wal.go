package store

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Streamer receives WAL entries for replication
type Streamer interface {
	Send(line string)
}

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	pending int           // writes since last fsync
	ticker  *time.Ticker  // group commit ticker
	done    chan struct{}
	streamer Streamer
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, 64*1024), // 64KB buffer
		ticker: time.NewTicker(time.Millisecond),
		done:   make(chan struct{}),
	}

	go w.runSync()
	return w, nil
}

func (w *WAL) SetStreamer(s Streamer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.streamer = s
}

// runSync flushes and fsyncs the WAL every millisecond if there are pending writes
func (w *WAL) runSync() {
	for {
		select {
		case <-w.ticker.C:
			w.mu.Lock()
			if w.pending > 0 {
				w.writer.Flush()
				w.file.Sync()
				w.pending = 0
			}
			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}

func (w *WAL) Write(op, key, value string, expireAt int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	line := fmt.Sprintf("%s\t%s\t%s\t%d", op, key, value, expireAt)
	_, err := fmt.Fprintln(w.writer, line)
	if err != nil {
		return err
	}
	w.pending++
	if w.streamer != nil {
		w.streamer.Send(line)
	}
	return nil
}

func (w *WAL) Replay(s *Store) error {
	// flush anything buffered before reading
	w.mu.Lock()
	w.writer.Flush()
	w.mu.Unlock()

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

	// fsync the compacted file before renaming
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	tmp.Close()

	if err := os.Rename(tmpPath, w.file.Name()); err != nil {
		os.Remove(tmpPath)
		return err
	}

	w.mu.Lock()
	newFile, err := os.OpenFile(w.file.Name(), os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.file.Close()
	w.file = newFile
	w.writer = bufio.NewWriterSize(newFile, 64*1024)
	w.mu.Unlock()

	return nil
}

func (w *WAL) Close() error {
	w.ticker.Stop()
	close(w.done)

	w.mu.Lock()
	defer w.mu.Unlock()
	w.writer.Flush()
	w.file.Sync()
	return w.file.Close()
}