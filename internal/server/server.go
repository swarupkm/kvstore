package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"kvstore/internal/mvcc"
	"kvstore/internal/store"
)

type Server struct {
	store         *store.Store
	mvcc          *mvcc.MVCCStore
	listener      net.Listener
	replicaOffset func() uint64
}

func New(addr string, s *store.Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		store:    s,
		mvcc:     mvcc.NewMVCCStore(),
		listener: ln,
	}, nil
}

func (s *Server) SetReplicaOffsetFunc(f func() uint64) {
	s.replicaOffset = f
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

// session holds per-connection state
type session struct {
	tx *mvcc.Tx // nil when outside a transaction
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	sess := &session{}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "BEGIN":
			if sess.tx != nil {
				fmt.Fprintln(conn, "ERR transaction already open")
				continue
			}
			sess.tx = s.mvcc.Begin(mvcc.ReadWrite)
			fmt.Fprintln(conn, "OK")

		case "COMMIT":
			if sess.tx == nil {
				fmt.Fprintln(conn, "ERR no open transaction")
				continue
			}
			// snapshot writes before commit clears them
			writes := sess.tx.Writes()
			if err := sess.tx.Commit(); err != nil {
				fmt.Fprintln(conn, "ERR", err)
				sess.tx = nil
				continue
			}
			// flush committed writes into WAL store for durability
			for key, v := range writes {
				if v.Deleted {
					s.store.Delete(key)
				} else if !v.ExpiresAt.IsZero() {
					ttl := time.Until(v.ExpiresAt)
					if ttl > 0 {
						s.store.SetWithTTL(key, v.Value, ttl)
					}
				} else {
					s.store.Set(key, v.Value)
				}
			}
    sess.tx = nil
    fmt.Fprintln(conn, "OK")

		case "ROLLBACK":
			if sess.tx == nil {
				fmt.Fprintln(conn, "ERR no open transaction")
				continue
			}
			sess.tx.Rollback()
			sess.tx = nil
			fmt.Fprintln(conn, "OK")

		case "SET":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR usage: SET <key> <value>")
				continue
			}
			if sess.tx != nil {
				// inside explicit transaction — buffer the write
				sess.tx.Set(parts[1], parts[2])
				fmt.Fprintln(conn, "OK")
			} else {
				// auto-commit via WAL store
				if err := s.store.Set(parts[1], parts[2]); err != nil {
					fmt.Fprintln(conn, "ERR", err)
					continue
				}
				// mirror into MVCC for consistent reads
				autoTx := s.mvcc.Begin(mvcc.ReadWrite)
				autoTx.Set(parts[1], parts[2])
				autoTx.Commit()
				fmt.Fprintln(conn, "OK")
			}

		case "GET":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR usage: GET <key>")
				continue
			}
			if sess.tx != nil {
				val, ok := sess.tx.Get(parts[1])
				if !ok {
					fmt.Fprintln(conn, "NULL")
					continue
				}
				fmt.Fprintln(conn, val)
			} else {
				val, ok := s.store.Get(parts[1])
				if !ok {
					fmt.Fprintln(conn, "NULL")
					continue
				}
				fmt.Fprintln(conn, val)
			}

		case "DEL":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR usage: DEL <key>")
				continue
			}
			if sess.tx != nil {
				sess.tx.Delete(parts[1])
				fmt.Fprintln(conn, "OK")
			} else {
				if err := s.store.Delete(parts[1]); err != nil {
					fmt.Fprintln(conn, "ERR", err)
					continue
				}
				autoTx := s.mvcc.Begin(mvcc.ReadWrite)
				autoTx.Delete(parts[1])
				autoTx.Commit()
				fmt.Fprintln(conn, "OK")
			}

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
			if sess.tx != nil {
				sess.tx.SetWithTTL(parts[1], parts[3], ttl)
				fmt.Fprintln(conn, "OK")
			} else {
				if err := s.store.SetWithTTL(parts[1], parts[3], ttl); err != nil {
					fmt.Fprintln(conn, "ERR", err)
					continue
				}
				autoTx := s.mvcc.Begin(mvcc.ReadWrite)
				autoTx.SetWithTTL(parts[1], parts[3], ttl)
				autoTx.Commit()
				fmt.Fprintln(conn, "OK")
			}

		case "INCR":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR usage: INCR <key>")
				continue
			}
			val, err := s.store.Increment(parts[1])
			if err != nil {
				fmt.Fprintln(conn, "ERR", err)
				continue
			}
			fmt.Fprintln(conn, val)

		case "KEYS":
			var keys []string
			if sess.tx != nil {
				keys = s.mvcc.Keys(sess.tx.ID)
			} else {
				keys = s.store.Keys()
			}
			if len(keys) == 0 {
				fmt.Fprintln(conn, "(empty)")
				fmt.Fprintln(conn, "")
				continue
			}
			for _, k := range keys {
				fmt.Fprintln(conn, k)
			}
			fmt.Fprintln(conn, "")

		case "RANGE":
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

		case "PREFIX":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR usage: PREFIX <prefix>")
				fmt.Fprintln(conn, "")
				continue
			}
			keys := s.store.Prefix(parts[1])
			if len(keys) == 0 {
				fmt.Fprintln(conn, "(empty)")
				fmt.Fprintln(conn, "")
				continue
			}
			for _, k := range keys {
				fmt.Fprintln(conn, k)
			}
			fmt.Fprintln(conn, "")

		case "COMPACT":
			if err := s.store.Compact(); err != nil {
				fmt.Fprintln(conn, "ERR", err)
				continue
			}
			fmt.Fprintln(conn, "OK")

		case "INFO":
			st := s.store.Stats()
			fmt.Fprintf(conn, "keys:            %d\n", st.Keys)
			fmt.Fprintf(conn, "sets:            %d\n", st.Sets)
			fmt.Fprintf(conn, "gets:            %d\n", st.Gets)
			fmt.Fprintf(conn, "deletes:         %d\n", st.Deletes)
			if s.replicaOffset != nil {
				fmt.Fprintf(conn, "replica_offset:  %d\n", s.replicaOffset())
			}

		case "STATS":
			st := s.store.Stats()
			fmt.Fprintf(conn, "keys:    %d\n", st.Keys)
			fmt.Fprintf(conn, "sets:    %d\n", st.Sets)
			fmt.Fprintf(conn, "gets:    %d\n", st.Gets)
			fmt.Fprintf(conn, "deletes: %d\n", st.Deletes)

		default:
			fmt.Fprintln(conn, "ERR unknown command")
		}
	}
}