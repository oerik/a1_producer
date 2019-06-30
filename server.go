package main

import (
    "net"
    "strconv"
    "fmt"
    "io/ioutil"
    "encoding/json"
    "context"
    kafka "github.com/segmentio/kafka-go"
)

const PORT = 3002

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func main() {
    kafkaWriter := getKafkaWriter("192.168.1.106:9092", "sensors")
    defer kafkaWriter.Close()

    server, err := net.Listen("tcp", ":" + strconv.Itoa(PORT))
    if server == nil {
        panic(err)
    }
    conns := clientConns(server)
    for {
        go handleConn(<-conns, kafkaWriter)
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

func handleConn(client net.Conn, kafkaWriter *kafka.Writer) {
	data,_ := ioutil.ReadAll(client)
	client.Close()
	var result map[string]interface{}
	json.Unmarshal([]byte(data),  &result)
	fmt.Printf("data %s", data)
	fmt.Printf("Content: ", result["version"], "button: ", result["button"])
	msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", result["type"])),
			Value: data,
			}
	err := kafkaWriter.WriteMessages(context.Background(), msg)
	fmt.Printf("error %s", err)
}

