package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Primary struct {
	mu       sync.Mutex
	replicas map[net.Conn]*bufio.Writer
	offset   atomic.Uint64
	buffer   *RingBuffer
}

func NewPrimary() *Primary {
	return &Primary{
		replicas: make(map[net.Conn]*bufio.Writer),
		buffer:   NewRingBuffer(10000), // keep last 10k entries
	}
}

func (p *Primary) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Println("replication listener on", addr)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handshake(conn)
		}
	}()
	return nil
}

// handshake reads the replica's last seen offset, replays missed entries,
// then adds it to the live fan-out set
func (p *Primary) handshake(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	// replica sends: OFFSET <last_seen>\n
	if !scanner.Scan() {
		conn.Close()
		return
	}

	parts := strings.Fields(scanner.Text())
	var replicaOffset uint64
	if len(parts) == 2 && parts[0] == "OFFSET" {
		replicaOffset, _ = strconv.ParseUint(parts[1], 10, 64)
	}

	fmt.Printf("replica connected from %s, last offset: %d (primary at: %d)\n",
		conn.RemoteAddr(), replicaOffset, p.offset.Load())

	// replay missed entries
	missed := p.buffer.Since(replicaOffset)
	for _, e := range missed {
		fmt.Fprintf(writer, "%d\t%s\n", e.Offset, e.Line)
	}
	writer.Flush()

	// add to live set
	p.mu.Lock()
	p.replicas[conn] = writer
	p.mu.Unlock()

	// detect disconnect
	go func() {
		buf := make([]byte, 1)
		conn.Read(buf)
		p.mu.Lock()
		delete(p.replicas, conn)
		p.mu.Unlock()
		fmt.Println("replica disconnected:", conn.RemoteAddr())
	}()
}

// Send stamps the entry with an offset, buffers it, and fans out to replicas
func (p *Primary) Send(line string) {
	offset := p.offset.Add(1)
	entry := Entry{Offset: offset, Line: line}
	p.buffer.Add(entry)

	p.mu.Lock()
	defer p.mu.Unlock()
	for conn, w := range p.replicas {
		if _, err := fmt.Fprintf(w, "%d\t%s\n", offset, line); err != nil {
			delete(p.replicas, conn)
			conn.Close()
			continue
		}
		w.Flush()
	}
}

func (p *Primary) CurrentOffset() uint64 {
	return p.offset.Load()
}