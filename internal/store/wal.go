package store

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type WAL struct {
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	f , err := os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Write(op, key, value string) error {
	_ , err := fmt.Fprintf(w.file, "%s\t%s\t%s\n", op, key, value)
	return err
}

func (w *WAL) Replay(s *Store) error {
	if _,err := w.file.Seek(0,0); err!= nil {
		return err
	}
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t" , 3)
		if len(parts) < 2 {
			continue
		}
		op, key := parts[0], parts[1]
		value := ""
		if len(parts) == 3 {
			value = parts[2]
		}

		switch op {
		case "SET":
			s.Set(key, value)
		case "DEL":
			s.Delete(key)
		case "INCR":
			s.Increment(key)
		}
	}
	return scanner.Err()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Compact(s *Store) error {
	s.mu.RLock()
	snapshot := make(map[string]string, len(s.data))
	for k, v := range s.data {
		snapshot[k] = v.value
	}
	s.mu.RUnlock()

	tmpPath := w.file.Name() + ".tmp"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(tmp)
	for k,v := range snapshot {
		if _, err := fmt.Fprintf(writer, "SET\t%s\t%s\n", k, v); err != nil {
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

	newFile, err := os.OpenFile(w.file.Name(), os.O_APPEND | os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	w.file.Close()
	w.file = newFile

	return nil
}