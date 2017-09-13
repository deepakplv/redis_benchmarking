// Set the redis_host before running the script.
// When using ElasticCache as redis_host, it should be in the same region as the publisher/consumer.
// Note: Before running parallel enque/deque, clear the redis cache to get correct latencies.
package main

import (
	"github.com/adjust/rmq"
	"fmt"
	"os"
	"time"
	"encoding/json"
	"sync/atomic"
	"sync"
	"bufio"
)

const (
	 //redis_host = "localhost:6379"
	redis_host = "benchmark-redis.tsfslr.ng.0001.euw2.cache.amazonaws.com:6379"

	redis_service_name = "benchmark-redis"
	redis_queue_name = "benchmark"
	messageCount = 10000
	prefetchLimit = 1000
	batchSize = 1000        // For batch consumer
	enque_parallelism = 10  // No. of gorutines for enqueuing
	deque_parallelism = 10  // No. of consumers for dequeuing

	// Time bound publish/deque
	runtimeDuration = time.Second * 60
	publishingWorkers = 300
)

func main() {
	taskQueue := createTaskQueue()

	mode := os.Getenv("mode")       // Use "export mode=p" or "export mode=c"
	if mode == "p" {
		fmt.Println("Starting publishing")
		BulkPublisher(taskQueue)
	} else if mode == "tbp" {
		fmt.Println("Starting time bound publishing for duration: ", runtimeDuration)
		TimeBoundPublisher(taskQueue, runtimeDuration)
	} else if mode == "c" {
		fmt.Println("Starting consuming")
		ConsumeMessages(taskQueue)
		select {}       // Keep consumer running
	} else if mode == "bc" {
		fmt.Println("Starting batch consumer")
		BatchConsumeMessages(taskQueue)
		select {}       // Keep batch consumer running
	} else if mode == "tbc" {
		fmt.Println("Starting time bound consumer for duration: ", runtimeDuration)
		TimeBoundConsumeMessages(taskQueue)
		select {}       // Keep batch consumer running
	} else {
		fmt.Println("Invalid Flag, Exiting")
		return
	}
}

func BulkPublisher(taskQueue rmq.Queue) {
	var count uint64 = 1
	var wg sync.WaitGroup
	wg.Add(enque_parallelism)
	for i:=0; i<enque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for ;count <= messageCount-enque_parallelism+1; {
				message := getMessage(count)
				publish(taskQueue, message)
				atomic.AddUint64(&count, 1)
			}
		}()
	}
	wg.Wait()
}

func TimeBoundPublisher(taskQueue rmq.Queue, totalDuration time.Duration) {
	var count uint64
	for i:=0; i<publishingWorkers; i++ {
		go func() {
			for {
				message := getMessage(count)
				publish(taskQueue, message)
				atomic.AddUint64(&count, 1)
				time.Sleep(time.Millisecond*975)
			}
		}()
	}
	select {
	        case <- time.After(totalDuration):
	                fmt.Println(count) // This may not be correct count as goroutines are still running
	                os.Exit(1)
        }
}

type Message struct {
	ID          uint64
	Payload     map[string]interface{}
	EnqueueTime uint64
}

func getMessage(id uint64) string{
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
	message := Message{ID: id, Payload: payload, EnqueueTime: uint64(time.Now().UnixNano())}
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

	atomic.AddUint64(&count, 1)
	atomic.AddUint64(&totalLatency, latency)

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

		atomic.AddUint64(&count, 1)
		atomic.AddUint64(&totalLatency, latency)

		// Set the startTime after first message is read, so that we can get enqueues per second
		if count == 1 {
			startTime = time.Now().UnixNano()
		}
	}

	// Acknowledge the batch delivery
	batch.Ack()
}

var latencyList []uint64
var initTime time.Time
func TimeBoundConsumeMessages(taskQueue rmq.Queue) {
	initTime = time.Now()
	// Start consuming and add consumers.
	taskQueue.StartConsuming(prefetchLimit, time.Millisecond)
	for i := 0; i < deque_parallelism; i++ {
		name := fmt.Sprintf("TimeBoundconsumer %d", i)
		taskQueue.AddConsumer(name, NewTimeBoundConsumer(i))
	}
}

type TimeBoundConsumer struct {
	name   string
}

func NewTimeBoundConsumer(tag int) *TimeBoundConsumer {
	return &TimeBoundConsumer{
		name:   fmt.Sprintf("TimeBoundconsumer%d", tag),
	}
}

var mutex = &sync.Mutex{}
func (consumer *TimeBoundConsumer) Consume(delivery rmq.Delivery) {
	// If runtime is over, write latencies to file and exit
	if time.Since(initTime) > runtimeDuration {
		mutex.Lock()    // Start critical section: To prevent multiple files creation
		// Create a file and write the latencies
		file, err := os.Create("/tmp/redis_latencies.txt")
		if err != nil {
			panic(err)
		}
		defer file.Close()
	        w := bufio.NewWriter(file)
		for _, latency := range latencyList {
	                fmt.Fprintln(w, latency)
	        }
		if err = w.Flush(); err != nil {
	                panic(err)
	        }

		fmt.Println("Average Latency for last item is ", totalLatency / count, count)
		os.Exit(1)
		mutex.Unlock()  // Ends critical section: Although unreachable due to exit above
	}

	// Dequeue the message and get the latency
	var message Message
	json.Unmarshal([]byte(delivery.Payload()), &message)
	enqueue_time := message.EnqueueTime
	latency := uint64(time.Now().UnixNano()) - enqueue_time

	atomic.AddUint64(&count, 1)
	atomic.AddUint64(&totalLatency, latency)
	// Add latency to list after converting from nano to millisecond
	latencyList = append(latencyList, latency/1000000)
	//fmt.Println("Message ID: ", message.ID)
	// Acknowledge the delivery
	delivery.Ack()
}