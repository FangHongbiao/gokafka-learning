package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	highLevel()
	// lowLevel()
}

func highLevel() {
	write()
	read()
}

func lowLevel() {
	produce()
	consume()
}

func produce() {
	// to produce messages
	topic := "my-topic"
	partition := 0

	fmt.Println("----")
	conn, err := kafka.DialLeader(context.Background(), "tcp", "39.106.208.186:9092", topic, partition)
	fmt.Println("----1")
	if err != nil {
		log.Fatal("连接失败：", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)

	conn.Close()
}

func consume() {
	// to consume messages
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "39.106.208.186:9092", topic, partition)
	if err != nil {
		log.Fatal("连接失败：", err)
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	batch.Close()
	conn.Close()
}

func write() {
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"39.106.208.186:9092"},
		Topic:    "topic-A",
		Balancer: &kafka.LeastBytes{},
	})

	fmt.Println("-----")
	w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	fmt.Println("-----")

	w.Close()
}

func read() {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"39.106.208.186:9092"},
		Topic:     "topic-A",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(0)

	fmt.Println("-----")
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
