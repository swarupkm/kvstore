package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
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
}

func NewReplica(primaryAddr string, store Applier) *Replica {
	return &Replica{primaryAddr: primaryAddr, store: store}
}

// Start connects to the primary and begins streaming WAL entries
func (r *Replica) Start() {
	go r.run()
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
	fmt.Println("connected to primary at", r.primaryAddr)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		if err := r.applyEntry(scanner.Text()); err != nil {
			fmt.Println("apply error:", err)
		}
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