package main

import (
    "net"
    "strconv"
    "fmt"
    "io/ioutil"
    "encoding/json"
)

const PORT = 3002

func main() {
    server, err := net.Listen("tcp", ":" + strconv.Itoa(PORT))
    if server == nil {
        panic(err)
    }
    conns := clientConns(server)
    for {
        go handleConn(<-conns)
    }
}

func clientConns(listener net.Listener) chan net.Conn {
    ch := make(chan net.Conn)
    i := 0
    go func() {
        for {
            client, err := listener.Accept()
            if client == nil {
                fmt.Printf("couldn't accept: ", err)
                continue
            }
            i++
            fmt.Printf("%d: %v <-> %v\n", i, client.LocalAddr(), client.RemoteAddr())
            ch <- client
        }
    }()
    return ch
}

func handleConn(client net.Conn) {
	data, err := ioutil.ReadAll(client)
	client.Close()
	var result map[string]interface{}
	json.Unmarshal([]byte(data),  &result)
	fmt.Printf("data %s %s", data, result)
	fmt.Printf("error %s", err)
}

