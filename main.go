package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Element represents data tag in some json collection
type Element struct {
	Key   string `json:"key,attr"`
	Value string `json:"value"`
}

// Collection contains an array of Elements
type Collection struct {
	Name     string     `json:"collection"`
	Elements []*Element `json:"element"`
	sync.Mutex
}

func getJsonFile(collection string, flags int) *os.File {
	file, err := os.OpenFile(collection+".json", flags, 777)
	if err != nil {
		fmt.Println(err)
	}
	return file
}

func getCollection(collection *os.File) *Collection {
	jsonReader := io.Reader(collection)
	collectionElement := &Collection{}
	if err := json.NewDecoder(jsonReader).Decode(collectionElement); err != nil {
		return nil
	}
	return collectionElement
}

func findCollision(elements []*Element, key, value string) bool {
	for _, element := range elements {
		if element.Key == key {
			element.Value = value
			return true
		}
	}
	return false
}

func (element *Element) saveElement(file *os.File) {
	collection := getCollection(file)
	if collection == nil {
		collection = &Collection{}
	}
	collection.Lock()
	defer collection.Unlock()
	if !findCollision(collection.Elements, element.Key, element.Value) {
		collection.Elements = append(collection.Elements, element)
		collection.Name = "example"
	}
	collection.saveElement(file)
}

// Create key-value in some json collection.
// Collection will be created if not exists
func set(c net.Conn, collectionName, key, value string) {
	file := getJsonFile(collectionName, os.O_CREATE|os.O_RDWR)
	defer file.Close()
	element := &Element{Key: key, Value: value}
	element.saveElement(file)
	if c != nil {
		c.Write([]byte(fmt.Sprintf("Key %s added to %s \n", key, collectionName)))
	}
}

func handleConnection(c net.Conn, msgchan chan<- string) {
	defer c.Close()
	fmt.Printf("Connection from %v established.\n", c.RemoteAddr())
	c.SetReadDeadline(time.Now().Add(time.Second * 10000))
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if (err != nil) || (n == 0) {
			c.Close()
			break
		}
		msgchan <- string(buf[0:n])
	}
	//fmt.Printf("Connection from %v closed.\n", c.RemoteAddr())
	c.Close()
	return
}

func getSetValue(params []string) (string, bool) {
	paramsCount := len(params)

	if paramsCount < 4 {
		fmt.Println("Count of arguments doesn't match required")
		return "", false
	}
	return strings.Join(params[3:], " "), true
}

func handleSet(c net.Conn, params []string) {
	value, valid := getSetValue(params)
	fmt.Println(params)
	if !valid {
		if c != nil {
			c.Write([]byte("Your 'set' query is incorrect.\n"))
		}
	} else {
		set(c, params[1], params[2], value)
	}
}

func elementFromCollection(collectionName, key string) *Element {
	file := getJsonFile(collectionName, os.O_RDONLY)
	defer file.Close()
	if file != nil {
		collection := getCollection(file)
		collection.Lock()
		defer collection.Unlock()
		for _, element := range collection.Elements {
			if element.Key == key {
				return element
			}
		}
	}
	return nil
}

func get(c net.Conn, collection, key string) string {
	element := elementFromCollection(collection, key)
	if element == nil {
		if c != nil {
			c.Write([]byte(fmt.Sprintf("No key '%s' in '%s'\n", key, collection)))
		}
		return ""
	}
	if c != nil {
		c.Write([]byte(fmt.Sprintf("Value: %s \n", element.Value)))
	}

	return element.Value
}

func handleGet(c net.Conn, params []string) string {
	paramsCount := len(params)
	if paramsCount != 3 {
		if c != nil {
			c.Write([]byte("Your 'get' query is incorrect.\n"))
		}
		return ""
	}

	return get(c, params[1], params[2])
}

func (collection *Collection) deleteKey(key string) bool {
	for i, element := range collection.Elements {
		if element.Key == key {
			collection.Elements = append(collection.Elements[:i],
				collection.Elements[i+1:]...)
			return true
		}
	}
	return false
}

func (collection *Collection) saveElement(file *os.File) {
	file.Seek(0, 0)
	file.Truncate(0)
	jsonWriter := io.Writer(file)
	enc := json.NewEncoder(jsonWriter)
	if err := enc.Encode(collection); err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

func delete(c net.Conn, collectionName, key string) {
	file := getJsonFile(collectionName, os.O_CREATE|os.O_RDWR)
	defer file.Close()
	collection := getCollection(file)
	collection.Lock()
	defer collection.Unlock()
	if collection != nil {
		if collection.deleteKey(key) {
			collection.saveElement(file)
			if c != nil {
				c.Write([]byte(fmt.Sprintf("Key '%s' was removed from '%s'\n", key, collectionName)))
			}
		} else {
			if c != nil {
				c.Write([]byte(fmt.Sprintf("Key '%s' wasn't found in '%s'\n", key, collectionName)))
			}
		}
	}
}

func handleDelete(c net.Conn, params []string) {
	paramsCount := len(params)
	if paramsCount != 3 {
		if c != nil {
			c.Write([]byte(fmt.Sprintf("DELETE query incorrect.\n")))
		}
	} else {
		delete(c, params[1], params[2])
	}
}

func handleDrop(c net.Conn, params []string) {
	paramsCount := len(params)
	file := getJsonFile(params[1], os.O_CREATE|os.O_RDWR)
	defer file.Close()
	collection := getCollection(file)
	collection.Lock()
	defer collection.Unlock()
	if paramsCount != 2 {
		if c != nil {
			c.Write([]byte("Drop query incorrect.\n"))
		}
	} else {
		err := os.Remove(params[1] + ".json")
		if err != nil {
			fmt.Println(err)
		} else {
			if c != nil {
				c.Write([]byte(fmt.Sprintf("Drop collection '%s'\n", params[1])))
			}
		}
	}
}

func handleQuery(c net.Conn, msgchan chan string) {
	for {
		msg := strings.TrimSpace(<-msgchan)
		query := strings.Fields(msg)
		switch query[0] {
		case "set":
			handleSet(c, query)
		case "get":
			handleGet(c, query)
		case "delete":
			handleDelete(c, query)
		case "drop":
			handleDrop(c, query)
		default:
			if c != nil {
				c.Write([]byte(fmt.Sprintf("Unknown command '%s'.\n", query[0])))
			}
		}
	}
}

func main() {
	ln, err := net.Listen("tcp", ":7777")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	messagechannel := make(chan string)

	for {
		conn, err := ln.Accept()
		go handleQuery(conn, messagechannel)

		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(conn, messagechannel)
	}
}
