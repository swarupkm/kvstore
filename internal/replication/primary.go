package replication

import (
	"bufio"
	"fmt"
	"net"
	"sync"
)

// Primary manages a set of replica connections and fans out WAL entries
type Primary struct {
	mu       sync.Mutex
	replicas map[net.Conn]*bufio.Writer
}

func NewPrimary() *Primary {
	return &Primary{
		replicas: make(map[net.Conn]*bufio.Writer),
	}
}

// Listen accepts incoming replica connections on the given address
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
			p.addReplica(conn)
		}
	}()
	return nil
}

func (p *Primary) addReplica(conn net.Conn) {
	p.mu.Lock()
	p.replicas[conn] = bufio.NewWriter(conn)
	p.mu.Unlock()
	fmt.Println("replica connected:", conn.RemoteAddr())

	// remove replica on disconnect
	go func() {
		buf := make([]byte, 1)
		conn.Read(buf) // blocks until disconnect
		p.mu.Lock()
		delete(p.replicas, conn)
		p.mu.Unlock()
		fmt.Println("replica disconnected:", conn.RemoteAddr())
	}()
}

// Send fans out a WAL entry to all connected replicas
func (p *Primary) Send(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for conn, w := range p.replicas {
		if _, err := fmt.Fprintln(w, line); err != nil {
			delete(p.replicas, conn)
			conn.Close()
			continue
		}
		w.Flush()
	}
}