// Erik Oomen, jul 2019
//
// This file opens a TCP listener at 3002 to receive json data.
// This json data is in DeviceFromat-1.0 and is published to
// kafka topic sensors.
//
// Warning: Not robust or designed for high performance.

package main

import (
    "net"
    "strconv"
    "fmt"
    "time"
    "io/ioutil"
    "encoding/json"
    "context"
    kafka "github.com/segmentio/kafka-go"
)

const (
	PORT = 3002
	KAFKA_SERVER    = "192.168.1.106:9092"
	SENSOR_TOPIC    = "sensors"
)

// Setup a kafka writer, tune for realtime pushes (50 msec)

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
	})
}


func main() {
    kafkaWriter := getKafkaWriter(KAFKA_SERVER, SENSOR_TOPIC)
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

// Accept a TCP socket
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

// Read TCP data, dump to the console and send to kafka.
func handleConn(client net.Conn, kafkaWriter *kafka.Writer) {
	data,_ := ioutil.ReadAll(client)
	client.Close()
	var result map[string]interface{}
	json.Unmarshal([]byte(data),  &result)
	fmt.Printf("data %s\n", data)
	fmt.Printf("Content: ", result["version"], "button: ", result["button"])
	msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("device-%s", result["Device_ID"])),
			Value: data,
			}
	err := kafkaWriter.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println("Error", err)
	}
}

