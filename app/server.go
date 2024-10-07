package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type RESPValue struct {
	arr []string
}

// Server Implementations
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

func parseRESPArr(respValue *RESPValue, respArr string) {
	reader := bufio.NewReader(strings.NewReader(respArr))

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "*") {
			respArrElement := line[1:]
			fmt.Printf("Number of elements: %s\n", respArrElement)

			count, err := strconv.Atoi(respArrElement)
			if err != nil {
				fmt.Println("Error converting count to int", err)
				break
			}

			respValue.arr = make([]string, count)

			for i := 0; i < count*2; i++ {
				line, err := reader.ReadString('\n')
				if err != nil {
					break
				}
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "$") {
					continue
				} else {
					respValue.arr[i/2] = line
				}
			}

			return
		}
	}

}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	fmt.Println("New connection from", conn.RemoteAddr())

	resp_value := RESPValue{}

	for {
		buffer := make([]byte, 1024)
		select {
		case <-s.quit:
			return
		default:
			n, err := conn.Read(buffer)
			if err != nil {
				fmt.Printf("Error reading from connection", err)
				return
			}

			reader := bufio.NewReader(strings.NewReader(string(buffer[:n])))

			firstByte, _ := reader.ReadByte()

			if firstByte == '*' && len(resp_value.arr) == 0 {
				parseRESPArr(&resp_value, string(buffer[:n]))
			} else if firstByte == '$' && len(resp_value.arr) == 0 {
				parseRESPArr(&resp_value, "*1\r\n"+string(buffer[:n]))
			}

			if resp_value.arr == nil {
				continue
			}

			if strings.ToLower(resp_value.arr[0]) == "echo" {
				echoCount := len(resp_value.arr[len(resp_value.arr)-1])
				finalOutput := fmt.Sprintf("$%d\r\n%s\r\n", echoCount, resp_value.arr[len(resp_value.arr)-1])
				conn.Write([]byte(finalOutput))
			} else {
				conn.Write([]byte("+PONG\r\n"))
			}

			resp_value.arr = nil
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
