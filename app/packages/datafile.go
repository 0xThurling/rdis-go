package packages

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/assert"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type FileReader struct {
	sync.RWMutex
}

func CreateFileHandler() *FileReader {
	return &FileReader{}
}

// GetRedisFile gets the file object for the database file
//
// It fetches the name and directory of the database file from the hash table,
// and opens the file. It asserts that there are no errors in the process.
//
// It returns the file object.
func (fr *FileReader) GetRedisFile(ht *HashTable) *os.File {
	dataFileDir, dirErr := ht.Get("config_dir")
	dataFileName, fileErr := ht.Get("config_dbfilename")

	assert.Assert(!dirErr, "Error getting config_dir - database file directory")
	assert.Assert(!fileErr, "Error getting config_dbfilename - database file name")

	filePath := filepath.Join(dataFileDir.(string), dataFileName.(string))
	println(filePath)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("Error: File was not created despite no errors: %s\n", filePath)
	}

	file, openErr := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	assert.Assert(openErr != nil, fmt.Sprintf("Error opening file %s: %v\n", filePath, openErr))

	return file
}

func (fr *FileReader) AddRedisFileMetaData(file *os.File) {
	redisVersion := "REDIS0001"

	_, err := file.Write([]byte(redisVersion))
	assert.Assert(err != nil, fmt.Sprintf("Error writing version header to database file: %v\n", err))
}

func (fr *FileReader) WriteAuxFields(file *os.File) {
	auxFields := map[string]string{
		"redis-ver":  "7.2.0",
		"redis-bits": "64",
		"ctime":      strconv.FormatInt(time.Now().Unix(), 10),
	}

	for k, v := range auxFields {
		_, err := file.Write([]byte{0xFA})
		assert.Assert(err != nil, fmt.Sprintf("Error writing aux field identifier to database file: %v\n", err))

		_, err = file.Write([]byte(k))
		assert.Assert(err != nil, fmt.Sprintf("Error writing aux field %s to database file: %v\n", k, err))

		_, err = file.Write([]byte(v))
		assert.Assert(err != nil, fmt.Sprintf("Error writing aux field %s to database file: %v\n", v, err))
	}
}

func (fr *FileReader) UpdateRedisFile(ht *HashTable) {
	fr.Lock()
	defer fr.Unlock()

	file := fr.GetRedisFile(ht)
	defer file.Close()

	fr.AddRedisFileMetaData(file)
	fr.WriteAuxFields(file)
}
