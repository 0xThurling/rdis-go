package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	handleConnection(conn)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	_ = conn.SetDeadline(time.Time{})

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Client disconnected")
			} else {
				log.Println("Error reading line: ", err)
			}
			return
		}

		input := strings.TrimSpace(line)

		fmt.Printf("%s", input)

		if input == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}
