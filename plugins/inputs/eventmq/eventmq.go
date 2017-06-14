package eventmq

// simple.go

import (
	"encoding/json"
	"fmt"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/errchan"
	"github.com/influxdata/telegraf/plugins/inputs"
	zmq "github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
	"log"
	"strings"
	"sync"
)

const defaultURL = "tcp://localhost:47293"

const DefaultTimeout = 5

const ROUTER_SHOW_WORKERS = "ROUTER_SHOW_WORKERS"
const STATUS = "STATUS"
const EMQP_VERSION = "eMQP/1.0"

type WaitingMessageCounts []string

type StatusResponse struct {
	WaitingMessageCounts WaitingMessageCounts  `json:"waiting_message_counts"`
	JobLatencies         map[string]JobLatency `json:"job_latencies"`
	ExecutedFunctions    map[string]int32      `json:"executed_functions"`
}

type JobLatency struct {
	Time      float32
	QueueName string
}

type ConnectedWorker struct {
	AvailableSlots int32       `json:"available_slots"`
	Hb             float32     `json:"hb"`
	Queues         []QueueInfo `json:"queues"`
}

type QueueInfo []interface {
}

type WorkersResponse struct {
	ConnectedQueues  map[string][]QueueInfo     `json:"connected_queues"`
	ConnectedWorkers map[string]ConnectedWorker `json:"connected_workers"`
}

type gatherFunc func(emq *EventMQ, acc telegraf.Accumulator, errChan chan error)

var gatherFunctions = []gatherFunc{gatherStatus, gatherQueues}

type EventMQ struct {
	Ok            bool
	URL           string
	Name          string
	ClientTimeout float64
	Queues        []string
	Workers       []string
}

func (emq *EventMQ) Request(command string, target interface{}) error {
	if emq.URL == "" {
		emq.URL = defaultURL
	}

	dealer := zmq.NewDealer(zmtp.NewSecurityNull(), generate_uuid())
	err := dealer.Connect(emq.URL)
	if err != nil {
		panic(err)
	}
	defer dealer.Close()

	send_command(dealer, command, target)
	return nil
}

func gatherStatus(emq *EventMQ, acc telegraf.Accumulator, errChan chan error) {
	status := &StatusResponse{}

	err := emq.Request(STATUS, &status)
	if err != nil {
		errChan <- err
		return
	}

	for _, element := range status.WaitingMessageCounts {
		name := strings.Split(element, ": ")[0]
		count := strings.Split(element, ": ")[1]
		acc.AddFields("eventmq_queue",
			map[string]interface{}{
				"waiting_messages": count,
			},
			map[string]string{
				"name": name,
			})

		// log.Printf("Queue: %s, Waiting Messages: %s", name, count)
	}

	errChan <- nil
}

func gatherQueues(emq *EventMQ, acc telegraf.Accumulator, errChan chan error) {
	workers := &WorkersResponse{}
	err := emq.Request(ROUTER_SHOW_WORKERS, &workers)
	if err != nil {
		errChan <- err
		return
	}

	// log.Printf("============ Connected Queues =================")
	for _, element := range workers.ConnectedQueues {
		for _, info := range element {
			queue, _ := info[1].(string)
			priority, _ := info[0].(float64)

			acc.AddFields("eventmq_queues",
				map[string]interface{}{
					"priority": priority,
				},
				map[string]string{
					"queue": queue,
				})
			// log.Printf("Queue %s ID: %s, priority: %d", key, queue, int(priority))
		}
	}

	// log.Printf("============ Connected Workers =================")
	for key, element := range workers.ConnectedWorkers {
		id := "unkown"
		hostname := "unkown"

		if len(strings.Split(key, ":")) > 1 {
			id = strings.Split(key, ":")[1]
			hostname = strings.Split(key, ":")[0]
		} else {
			id = key
		}

		acc.AddFields("eventmq_workers",
			map[string]interface{}{
				"available_slots": element.AvailableSlots,
			},
			map[string]string{
				"id":       id,
				"hostname": hostname,
			})
		// log.Printf("Worker %s: Slots Available: %d, HB: %f", key, element.AvailableSlots, element.Hb)
	}
}

func (emq *EventMQ) Description() string {
	return "Read metrics from an EventtMQ server via the management port"
}

func (emq *EventMQ) SampleConfig() string {
	var sampleConfig = `
	# url = "tcp://localhost:47293"
`

	return sampleConfig
}

func (emq *EventMQ) Gather(acc telegraf.Accumulator) error {
	var wg sync.WaitGroup
	wg.Add(len(gatherFunctions))
	errChan := errchan.New(len(gatherFunctions))
	for _, f := range gatherFunctions {
		go func(gf gatherFunc) {
			defer wg.Done()
			gf(emq, acc, errChan.C)
		}(f)
	}
	wg.Wait()

	return errChan.Error()
}

func generate_uuid() string {
	uuid, err := zmq.NewUUID()
	if err != nil {
	}

	return uuid
}

func send_command(socket zmq.Dealer, command string, target interface{}) [][]byte {

	msg := make([][]byte, 5)
	msg[0] = []byte("")
	msg[1] = []byte(EMQP_VERSION)
	msg[2] = []byte(command)
	msg[3] = []byte(fmt.Sprintf("admin:%s", generate_uuid()))

	// log.Printf("Sending message: %s", msg)
	socket.SendMultipart(msg)

	reply, err := socket.RecvMultipart()
	if err != nil {
	}

	// log.Printf(string(reply[5]))

	switch command {
	case STATUS:
		// resp := &StatusResponse{}
		json.Unmarshal(reply[5], target)

		// log.Printf("============ Status =================")
		// for _, element := range resp.WaitingMessageCounts {
		// name := strings.Split(element, ": ")[0]
		// count := strings.Split(element, ": ")[1]
		// log.Printf("Queue: %s, Waiting Messages: %s", name, count)
		// }

	case ROUTER_SHOW_WORKERS:
		// resp := &WorkersResponse{}
		json.Unmarshal(reply[5], target)

		// log.Printf("============ Connected Queues =================")
		// for key, element := range resp.ConnectedQueues {
		// 	for _, info := range element {
		// 		queue, _ := info[1].(string)
		// 		priority, _ := info[0].(float64)
		// 		// log.Printf("Queue %s ID: %s, priority: %d", key, queue, int(priority))
		// 	}
		// }

		// log.Printf("============ Connected Workers =================")
		// for key, element := range resp.ConnectedWorkers {
		// log.Printf("Worker %s: Slots Available: %d, HB: %f", key, element.AvailableSlots, element.Hb)
		// }
	default:
		log.Printf("Unknown command: %s", command)
	}

	return reply
}

func init() {
	inputs.Add("eventmq", func() telegraf.Input { return &EventMQ{} })
}
