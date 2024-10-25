package main

import (
	"bufio"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/assert"
	"github.com/codecrafters-io/redis-starter-go/app/packages"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
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

func (s *Server) Start(ht *packages.HashTable) {
	s.wg.Add(1)
	go s.Loop(ht)
}

func (s *Server) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) Loop(ht *packages.HashTable) {
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
				go s.handleConnection(conn, ht)
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

func checkIfArrayIndexExists(arr RESPValue, index int) bool {
	return len(arr.arr) > index
}

func getConfigInformation(key []string, ht *packages.HashTable) (interface{}, bool) {
	if strings.ToLower(key[0]) == "get" {
		configKey := fmt.Sprintf("config_%s", key[1])
		return ht.Get(configKey)
	}
	return nil, false
}

func (s *Server) handleConnection(conn net.Conn, ht *packages.HashTable) {
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
			} else if strings.ToLower(resp_value.arr[0]) == "set" {
				if checkIfArrayIndexExists(resp_value, 4) {
					if strings.ToLower(resp_value.arr[3]) == "px" {
						exp, err := strconv.Atoi(resp_value.arr[4])
						if err != nil {
							println("Error converting exp to int while setting expiration date", err)
						}
						ht.Insert(resp_value.arr[1], resp_value.arr[2], &exp, time.Now())
					}
				} else {
					ht.Insert(resp_value.arr[1], resp_value.arr[2], nil, time.Now())
				}
				conn.Write([]byte("+OK\r\n"))
			} else if strings.ToLower(resp_value.arr[0]) == "get" {
				value, found := ht.Get(resp_value.arr[1])
				if found {
					count := len(value.(string))
					finalOutput := fmt.Sprintf("$%d\r\n%s\r\n", count, value.(string))
					conn.Write([]byte(finalOutput))
				} else {
					conn.Write([]byte("$-1\r\n"))
				}
			} else if strings.ToLower(resp_value.arr[0]) == "config" {
				info, found := getConfigInformation(resp_value.arr[1:], ht)
				fmt.Println(info)
				if found {
					count := len(info.(string))
					finalOutput := fmt.Sprintf("*2\r\n$3\r\n%s\r\n$%d\r\n%s\r\n", resp_value.arr[len(resp_value.arr)-1], count, info.(string))
					conn.Write([]byte(finalOutput))
				}
			} else if strings.ToLower(resp_value.arr[0]) == "save" {
				fh := packages.CreateFileHandler()
				fh.UpdateRedisFile(ht)
			} else {
				conn.Write([]byte("+PONG\r\n"))
			}

			resp_value.arr = nil
		}
	}
}

func handleCliArguments(args []string, ht *packages.HashTable) {
	for i, arg := range args {
		if arg == "--dir" {
			if _, err := os.Stat(args[i+1]); os.IsNotExist(err) {
				fmt.Printf("Directory doesn't exist\r\n")
				ht.Insert("config_dir", args[i+1], nil, time.Now())
				os.Mkdir(args[i+1], os.ModePerm)
			} else if _, found := ht.Get("dir"); !found {
				fmt.Println("Directory Exists: Adding key\n")
				ht.Insert("config_dir", args[i+1], nil, time.Now())
			} else {
				fmt.Println("Directory Exists\n")
			}
		}
		if arg == "--dbfilename" {
			dir, htErr := ht.Get("config_dir")

			assert.Assert(!htErr, "Failed to get the directory")

			filePath := filepath.Join(dir.(string), args[i+1])
			println(filePath)
			file, err := os.Create(filePath)
			if err != nil {
				fmt.Println("Error creating file", err)
			}

			defer file.Close()
			fmt.Println("File created successfully\n")

			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				fmt.Printf("Error: File was not created despite no errors: %s\n", filePath)
				continue
			}
			ht.Insert("config_dbfilename", args[i+1], nil, time.Now())
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	ht := packages.NewHashTable(30)

	handleCliArguments(os.Args[1:], ht)

	// Uncomment this block to pass the first stage
	server, err := NewServer(":6379")
	if err != nil {
		fmt.Println("Error creating server", err)
		return
	}

	server.Start(ht)

	fmt.Println("Press Enter to stop the server...")
	fmt.Scanln()

	server.Stop()
}
