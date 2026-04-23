package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"kvstore/internal/store"
)

type Server struct {
	store    *store.Store
	listener net.Listener
}

func New(addr string, s *store.Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{store: s, listener: ln}, nil
}

func (s *Server) Start() {
	fmt.Println("kvstore listening on", s.listener.Addr())
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "SET":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR usage: SET <key> <value>")
				continue
			}
			if err := s.store.Set(parts[1], parts[2]); err != nil {
				fmt.Fprintln(conn, "ERR", err)
				continue
			}
			fmt.Fprintln(conn, "OK")

		case "GET":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR usage: GET <key>")
				continue
			}
			val, ok := s.store.Get(parts[1])
			if !ok {
				fmt.Fprintln(conn, "NULL")
				continue
			}
			fmt.Fprintln(conn, val)
		case "DEL":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR usage: DEL <key>")
				continue
			}
			if err := s.store.Delete(parts[1]); err != nil {
				fmt.Fprintln(conn, "ERR", err)
				continue
			}
			fmt.Fprintln(conn, "OK")
		case "COMPACT":
			if err := s.store.Compact(); err != nil {
				fmt.Fprintln(conn, "ERR",  err)
				continue
			}
			fmt.Fprintln(conn, "OK")
		case "SETEX":
			if len(parts) != 4 {
				fmt.Fprintln(conn, "ERR usage: SETEX <key> <seconds> <value>")
				continue
			}
			secs, err := strconv.Atoi(parts[2])
			if err != nil || secs <= 0 {
				fmt.Fprintln(conn, "ERR seconds must be a positive integer")
				continue
			}
			ttl := time.Duration(secs) * time.Second
			if err := s.store.SetWithTTL(parts[1], parts[3], ttl); err != nil {
				fmt.Fprintln(conn, "ERR", err)
				continue
			}
			fmt.Fprintln(conn, "OK")
		case "KEYS":
			keys := s.store.Keys()
			if len(keys) == 0 {
				fmt.Fprintln(conn, "(empty)")
				fmt.Fprintln(conn, "") // sentinel
				continue
			}
			for _, k := range keys {
				fmt.Fprintln(conn, k)
			}
			fmt.Fprintln(conn, "") // sentinel — blank line signals end of list
		case "STATS":
			st := s.store.Stats()
			fmt.Fprintf(conn, "keys:    %d\n", st.Keys)
			fmt.Fprintf(conn, "sets:    %d\n", st.Sets)
			fmt.Fprintf(conn, "gets:    %d\n", st.Gets)
			fmt.Fprintf(conn, "deletes: %d\n", st.Deletes)
		case "RANGE":
			// RANGE <start> <end>
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR usage: RANGE <start> <end>")
				fmt.Fprintln(conn, "")
				continue
			}
			keys := s.store.Range(parts[1], parts[2])
			if len(keys) == 0 {
				fmt.Fprintln(conn, "(empty)")
				fmt.Fprintln(conn, "")
				continue
			}
			for _, k := range keys {
				fmt.Fprintln(conn, k)
			}
			fmt.Fprintln(conn, "")
		default:
			fmt.Fprintln(conn, "ERR unknown command")
		}
	}
}
