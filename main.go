package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/carambula84/pg_logical/models"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	postgresConstring        = "host=127.0.0.1 user=testuser password=example dbname=vadym sslmode=disable"
	PglogicalReplicationSlot = "test_replication"
	KAFKA_BROKER             = "localhost:9092"
	DEFAULT_KAFKA_TOPIC      = "test"
	MAX_KAFKA_MESSAGE_SIZE   = 1100000
	MAX_KAFKA_MESSAGES       = 5000
)

type inStream interface {
	GetChannelInputStream() *chan string
	GracefullyClose()
}

// LDdata for full serialisation of postgres logical decoding messages:
type LDdata struct {
	Change []struct {
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		Columnnames  []string      `json:"columnnames"`
		Columntypes  []string      `json:"columntypes"`
		Columnvalues []interface{} `json:"columnvalues"`
	} `json:"change"`
}

func main() {
	// initialise logical replication connection
	LRDpostgres := models.FactoryPGLogicalInputStream(postgresConstring, PglogicalReplicationSlot, 5000)
	defer LRDpostgres.GracefullyClose()
	// chanel of logical replication data
	inp := LRDpostgres.GetChannelInputStream()

	// get kafka topic from command-line argument
	var kafkaTopic string
	if len(os.Args) > 1 {
		kafkaTopic = os.Args[1]
	} else {
		kafkaTopic = DEFAULT_KAFKA_TOPIC
	}

	// create kafka producer
	KP, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KAFKA_BROKER})
	if err != nil {
		log.Panic("Failed to create kafka producer for topic " + kafkaTopic + ", on " + KAFKA_BROKER)
	}
	prodChan := KP.ProduceChannel()

	// start logger to listen to kafka meesages in background:
	go kafkaLogger(KP)

	// connect logical replication chanel to kafka input chanel
	for r := range inp {
		message := []byte(r)
		//fmt.Println("DEBUG: message size: ", len(message))
		// make sure message is not larger MAX_KAFKA_MESSAGE_SIZE
		if len(message) > MAX_KAFKA_MESSAGE_SIZE {
			pushLDdataInParts(message, prodChan, kafkaTopic)
		} else {
			prodChan <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny}, Value: message}
		}
	}
}

// pushLDdataInParts separates the message into pieces if message exceedes kafka message limit
// the least efficient part, because does serialsation of strings into jsons, split, and then serialisation back
func pushLDdataInParts(pmessage []byte, prodChan chan *kafka.Message, kafkaTopic string) {

	var lgMessages LDdata
	if err := json.Unmarshal(pmessage, &lgMessages); err != nil {
		panic(err)
	}
	meslen := len(lgMessages.Change)
	//fmt.Println("DEBUG: ", lgMessages.Change[meslen-5 : meslen-1])
	//fmt.Println("DEBUG: ", meslen = ", meslen)
	i := 0
	j := min(MAX_KAFKA_MESSAGES, meslen)
	for m := 2; i < j; m++ {
		stride := lgMessages.Change[i:j]

		slices := LDdata{
			Change: stride,
		}
		tex, _ := json.Marshal(slices)
		// uncomment for debugging:
		// os.Stdout.Write(tex)    // go run main.go > log.log
		//fmt.Println("SIZE subslice = ", len([]byte(tex)))
		prodChan <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny}, Value: []byte(tex)}

		i = j
		j = min(MAX_KAFKA_MESSAGES*m, meslen-1)
	}
}

func kafkaLogger(KP *kafka.Producer) {
	fmt.Println("kafka logger started")

	for {
	outer:
		for e := range KP.Events() {
			// filter for messages we care:
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("LOG-kafka: Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("LOG-kafka: Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				break outer

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
