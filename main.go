// Set the redis_host before running the script.
// When using ElasticCache as redis_host, it should be in the same region as the publisher/consumer.
package main

import (
	"github.com/adjust/rmq"
	"fmt"
	"os"
	"time"
	"encoding/json"
	"sync/atomic"
	"sync"
)

const (
	redis_host = "localhost:6379"
	redis_service_name = "benchmark-redis"
	redis_queue_name = "benchmark"
	messageCount = 10000
	prefetchLimit = 1000
	batchSize = 1000
	enque_parallelism = 10  // No. of gorutines for enqueuing
	deque_parallelism = 10  // No. of consumers for dequeuing
)

func main() {
	taskQueue := createTaskQueue()
	message := getMessage()

	mode := os.Getenv("mode")       // Use "export mode=p" or "export mode=c"
	if mode == "p" {
		fmt.Println("Starting publishing")
		BulkPublisher(taskQueue, message)
	} else if mode == "c" {
		fmt.Println("Starting consuming")
		ConsumeMessages(taskQueue)
		select {}       // Keep consumer running
	} else if mode == "bc" {
		fmt.Println("Starting batch consumer")
		BatchConsumeMessages(taskQueue)
		select {}       // Keep batch consumer running
	} else {
		fmt.Println("Invalid Flag, Exiting")
		return
	}
}

func BulkPublisher(taskQueue rmq.Queue, message string) {
	var count uint64 = 1
	var wg sync.WaitGroup
	wg.Add(enque_parallelism)
	for i:=0; i<enque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for ;count <= messageCount-enque_parallelism+1; {
				publish(taskQueue, message)
				atomic.AddUint64(&count, 1)
			}
		}()
	}
	wg.Wait()
}

type Message struct {
	Payload   map[string]interface{}
	EnqueueTime uint64
}

func getMessage() string{
	payload := map[string]interface{}{
	        "src": "972525626731",
	        "dst": "972502224696",
	        "prefix": "972502224696",
	        "url": "",
	        "method": "POST",
	        "text": "\u05dc\u05e7\u05d5\u05d7 \u05d9\u05e7\u05e8 \u05e2\u05e7\u05d1 \u05ea\u05e7\u05dc\u05d4 STOP",
	        "log_sms": "true",
	        "message_uuid": "ffe2bb44-d34f-4359-a7d7-217bf4e9f705",
	        "message_time": "2017-07-13 13:12:47.046303",
	        "carrier_rate": "0.0065",
	        "carrier_amount": "0.013",
	        "is_gsm": false,
	        "is_unicode": true,
	        "units": "2",
	        "auth_info": map[string]interface{}{
		        "auth_id": "MANZE1ODRHYWFIZGMXNJ",
			"auth_token": "NWRjNjU3ZDJhZDM0ZjE5NWE5ZWRmYTNmOGIzNGZm",
			"api_id": "de124d64-6186-11e7-920b-0600a1193e9b",
			"api_method": "POST",
			"api_name": "/api/v1/Message/",
			"account_id": "48844",
			"subaccount_id": "0",
			"parent_auth_id": "MANZE1ODRHYWFIZGMXNJ",
	        },
	}
	message := Message{Payload: payload, EnqueueTime: uint64(time.Now().UnixNano())}
	messageBytes, _ := json.Marshal(message)
	return string(messageBytes)
}

func createTaskQueue() rmq.Queue {
	connection := rmq.OpenConnection(redis_service_name, "tcp", redis_host, 0)
	taskQueue := connection.OpenQueue(redis_queue_name)
	return taskQueue
}

func publish(taskQueue rmq.Queue, message string) {
	// Enqueue task
	taskQueue.Publish(message)
}

func ConsumeMessages(taskQueue rmq.Queue) {
	// Start consuming and add consumers.
	taskQueue.StartConsuming(prefetchLimit, time.Millisecond)
	for i := 0; i < deque_parallelism; i++ {
		name := fmt.Sprintf("consumer %d", i)
		taskQueue.AddConsumer(name, NewConsumer(i))
	}
}

type Consumer struct {
	name   string
}

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		name:   fmt.Sprintf("consumer%d", tag),
	}
}

var totalLatency, count uint64
var startTime int64
func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	// Dequeue the message and get the latency
	var message Message
	json.Unmarshal([]byte(delivery.Payload()), &message)
	enqueue_time := message.EnqueueTime
	latency := uint64(time.Now().UnixNano()) - enqueue_time

	// Average out the latencies
	if count >= messageCount-1 {
		fmt.Println("Average Latency for last item is ", totalLatency / count, count)
		timeElapsedSeconds := float64(time.Now().UnixNano() - startTime)/float64(time.Second)
		fmt.Println("No. of messages dequeued per second: ", float64(1)/(timeElapsedSeconds/float64(messageCount-1)))
		os.Exit(1)
	}
	if latency != 0 {
		atomic.AddUint64(&count, 1)
		atomic.AddUint64(&totalLatency, latency)
	}

	// Set the startTime after first message is read, so that we can get enqueues per second
	if count == 1 {
		startTime = time.Now().UnixNano()
	}
	//fmt.Printf("%s consumed message: %s\n", consumer.name, message.Payload)
	// Acknowledge the delivery
	delivery.Ack()
}

func BatchConsumeMessages(taskQueue rmq.Queue) {
	// Start consuming and add consumers.
	taskQueue.StartConsuming(prefetchLimit, time.Millisecond)
	name := "batchconsumer 1"
	taskQueue.AddBatchConsumer(name, batchSize, NewBatchConsumer(1))
}

type BatchConsumer struct {
	name string
}

func NewBatchConsumer(tag int) *BatchConsumer {
	return &BatchConsumer{
		name:   fmt.Sprintf("batchconsumer%d", tag),
	}
}

func (consumer *BatchConsumer) Consume(batch rmq.Deliveries) {
	// fmt.Printf("%s consumed %d\n", consumer.name, len(batch))

	// Dequeue the messages and get the latency
	var message Message
	for i:=0; i<len(batch); i++ {
		currentMessage := batch[i]
		json.Unmarshal([]byte(currentMessage.Payload()), &message)
		enqueue_time := message.EnqueueTime
		latency := uint64(time.Now().UnixNano()) - enqueue_time

		// Average out the latencies
		if count >= messageCount - 1 {
			fmt.Println("Average Latency for last item is ", totalLatency / count, count)
			timeElapsedSeconds := float64(time.Now().UnixNano() - startTime) / float64(time.Second)
			fmt.Println("No. of messages dequeued per second: ", float64(1) / (timeElapsedSeconds / float64(messageCount - 1)))
			os.Exit(1)
		}
		if latency != 0 {
			atomic.AddUint64(&count, 1)
			atomic.AddUint64(&totalLatency, latency)
		}

		// Set the startTime after first message is read, so that we can get enqueues per second
		if count == 1 {
			startTime = time.Now().UnixNano()
		}
	}

	// Acknowledge the batch delivery
	batch.Ack()
}
