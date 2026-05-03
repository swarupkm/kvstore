package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

func main() {
	const (
		workers    = 20
		opsEach    = 500
		addr       = "localhost:5379"
	)

	var wg sync.WaitGroup
	start := time.Now()
	var totalOps int64 = workers * opsEach

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fmt.Println("connect error:", err)
				return
			}
			defer conn.Close()

			writer := bufio.NewWriter(conn)
			scanner := bufio.NewScanner(conn)

			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("worker%d:key%d", id, i)

				// SET
				fmt.Fprintf(writer, "SET %s value%d\n", key, i)
				writer.Flush()
				scanner.Scan()

				// GET
				fmt.Fprintf(writer, "GET %s\n", key)
				writer.Flush()
				scanner.Scan()
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	opsPerSec := float64(totalOps*2) / elapsed.Seconds()
	fmt.Printf("workers: %d | ops: %d | time: %s | throughput: %.0f ops/sec\n",
		workers, totalOps*2, elapsed.Round(time.Millisecond), opsPerSec)
}