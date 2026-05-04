package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Applier interface {
	Set(key, value string) error
	SetWithTTL(key, value string, ttl time.Duration) error
	Delete(key string) error
	Increment(key string) (int64, error)
}

type Replica struct {
	primaryAddr string
	store       Applier
	offset      atomic.Uint64
}

func NewReplica(primaryAddr string, store Applier) *Replica {
	return &Replica{primaryAddr: primaryAddr, store: store}
}

func (r *Replica) Start() {
	go r.run()
}

func (r *Replica) Offset() uint64 {
	return r.offset.Load()
}

func (r *Replica) run() {
	for {
		if err := r.connect(); err != nil {
			fmt.Println("replication error, retrying in 1s:", err)
			time.Sleep(time.Second)
		}
	}
}

func (r *Replica) connect() error {
	conn, err := net.Dial("tcp", r.primaryAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	scanner := bufio.NewScanner(conn)

	// send our current offset so primary can replay missed entries
	currentOffset := r.offset.Load()
	fmt.Fprintf(writer, "OFFSET %d\n", currentOffset)
	writer.Flush()
	fmt.Printf("connected to primary, sending offset: %d\n", currentOffset)

	for scanner.Scan() {
		line := scanner.Text()
		// wire format: "<offset>\t<op>\t<key>\t<value>\t<expireAt>"
		idx := strings.Index(line, "\t")
		if idx == -1 {
			continue
		}
		offsetStr := line[:idx]
		entry := line[idx+1:]

		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			continue
		}

		if err := r.applyEntry(entry); err != nil {
			fmt.Println("apply error:", err)
			continue
		}

		// advance our offset
		r.offset.Store(offset)
	}
	return scanner.Err()
}

func (r *Replica) applyEntry(line string) error {
	parts := strings.SplitN(line, "\t", 4)
	if len(parts) < 2 {
		return nil
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
				return r.store.SetWithTTL(key, value, ttl)
			}
			return nil
		}
		return r.store.Set(key, value)
	case "DEL":
		return r.store.Delete(key)
	case "INCR":
		_, err := r.store.Increment(key)
		return err
	}
	return nil
}