package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

type Server struct {
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
}

func NewServer(address string) (*Server, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	return &Server{
		listener: l,
		quit:     make(chan struct{}),
	}, nil
}

func (s *Server) Start() {
	s.wg.Add(1)
	go s.Loop()
}

func (s *Server) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) Loop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.quit:
			return
		default:
			conn, err := s.listener.Accept()

			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					fmt.Println("Error accepting connection", err)
				}
			} else {
				s.wg.Add(1)
				go s.handleConnection(conn)
			}
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	fmt.Println("New connection from", conn.RemoteAddr())
	buffer := make([]byte, 1024)

	for {
		select {
		case <-s.quit:
			return
		default:
			n, err := conn.Read(buffer)
			if err != nil {
				fmt.Println("Error reading from connection", err)
				return
			}

			fmt.Printf("Received %d bytes: %s\n", n, string(buffer[:n]))

			if strings.Contains(string(buffer[:n]), "PING") {
				_, err = conn.Write([]byte("+PONG\r\n"))
			}
			if err != nil {
				fmt.Println("Error writing to connection", err)
				return
			}
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	server, err := NewServer(":6379")
	if err != nil {
		fmt.Println("Error creating server", err)
		return
	}

	server.Start()

	fmt.Println("Press Enter to stop the server...")
	fmt.Scanln()

	server.Stop()
}
