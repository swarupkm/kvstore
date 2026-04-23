package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

const prompt = "kvstore> "

func main() {
	addr := "localhost:5379"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to %s\n", addr)
	fmt.Println("Commands: SET <key> <value> | GET <key> | DEL <key> | SETEX <key> <seconds> <value> | INCR <key> | KEYS | STATS | COMPACT | EXIT")
	fmt.Println()

	input := bufio.NewScanner(os.Stdin)
	response := bufio.NewScanner(conn)

	for {
		fmt.Print(prompt)

		if !input.Scan() {
			break
		}

		line := strings.TrimSpace(input.Text())
		if line == "" {
			continue
		}
		if strings.ToUpper(line) == "EXIT" {
			fmt.Println("bye.")
			break
		}

		// send command to server
		fmt.Fprintln(conn, line)

		// read response lines until a blank line or single-line response
		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "KEYS", "RANGE":
			// multi-line response — read until blank line sentinel
			for response.Scan() {
				text := response.Text()
				if text == "" {
					break
				}
				fmt.Println(text)
			}
		case "STATS":
			// 4 lines of output
			for i := 0; i < 4; i++ {
				if response.Scan() {
					fmt.Println(response.Text())
				}
			}
		default:
			// single line response
			if response.Scan() {
				fmt.Println(response.Text())
			}
		}
	}
}