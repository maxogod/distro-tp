package main

import (
	"log"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/filter/test/mock"
	"google.golang.org/protobuf/proto"
)

const amountOfMessages = 10

func main() {

	finishQueue, err := middleware.NewExchangeMiddleware("amqp://guest:guest@localhost:5672/", "finish_exchange", "direct", []string{"filter"})

	NodeConnectionsQueue, err := middleware.NewQueueMiddleware("amqp://guest:guest@localhost:5672/", "node_connections")
	ProcessedDataQueue, err := middleware.NewQueueMiddleware("amqp://guest:guest@localhost:5672/", "processed_data")

	if err != nil {
		log.Fatal(err)
	}
	defer finishQueue.Close()
	defer NodeConnectionsQueue.Close()
	defer ProcessedDataQueue.Close()

	// await for announcement
	done := make(chan bool, 1)

	var workerName string

	log.Println("Awaiting controller announcement")
	e := NodeConnectionsQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {

			announceMsg := &controller_connection.ControllerConnection{}

			err := proto.Unmarshal(msg.Body, announceMsg)

			workerName = announceMsg.WorkerName

			if err != nil {
				log.Fatalf("Failed to unmarshal controller connection message: %v", err)
			}

			log.Printf("Received a controller connection message: WorkerName=%s, Done=%v", announceMsg.WorkerName, announceMsg.Finished)

			msg.Ack(false)

			break
		}
		done <- true
	})
	<-done

	NodeConnectionsQueue.StopConsuming()

	// send data bataches
	log.Println("Sending data batches to filter")

	dataQueue, err := middleware.NewExchangeMiddleware("amqp://guest:guest@localhost:5672/", "finish_exchange", "direct", []string{workerName})
	defer dataQueue.Close()

	for i := 0; i < amountOfMessages; i++ {

		transactionBatch := &raw.TransactionBatch{
			Transactions: mock.MockTransactions,
		}
		payload, err := proto.Marshal(transactionBatch)
		if err != nil {
			log.Fatalf("Failed to marshal transaction batch: %v", err)
		}

		dataBatch := &data_batch.DataBatch{
			TaskType: int32(enum.T1),
			Done:     false,
			Payload:  payload,
		}

		dataPayload, err := proto.Marshal(dataBatch)
		if err != nil {
			log.Fatalf("Failed to marshal data batch: %v", err)
		}

		e := dataQueue.Send(dataPayload)
		if e != middleware.MessageMiddlewareSuccess {
			log.Fatalf("Failed to send message to filter queue: %v", e)
		}

		log.Println("Message sent to filter queue")
	}

	time.Sleep(1 * time.Second)
	// now we send a DONE to the filter via finish_exchange
	log.Println("Sending done message to finish exchange")
	finish := &data_batch.DataBatch{
		Done: true,
	}

	finishPayload, err := proto.Marshal(finish)
	if err != nil {
		log.Fatalf("Failed to marshal data batch: %v", err)
	}

	e = finishQueue.Send(finishPayload)
	if e != middleware.MessageMiddlewareSuccess {
		log.Fatalf("Failed to send message to finish queue: %v", e)
	}

	log.Println("Message sent to finish queue")

	// Now we check that we got a done from the filter

	log.Println("Awaiting done message from filter")
	done = make(chan bool, 1)

	e = NodeConnectionsQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {

			announceMsg := &controller_connection.ControllerConnection{}

			err := proto.Unmarshal(msg.Body, announceMsg)
			if err != nil {
				log.Fatalf("Failed to unmarshal controller connection message: %v", err)
			}

			log.Printf("Received a controller connection message: WorkerName=%s, Done=%v", announceMsg.WorkerName, announceMsg.Finished)

			msg.Ack(false)

			break
		}
		done <- true
	})
	<-done

	log.Println("Finishing mock controller")

}
