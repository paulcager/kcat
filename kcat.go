// kcat prints all topic messages to stdout. All topics will be printed, except for internal ones (beginning with "__").
package main

import (
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	flag "github.com/spf13/pflag"
)

const (
	// RFC3339Milli formats times as Milli-second precision timestamps.
	RFC3339Milli = "2006-01-02T15:04:05.000Z"
)

var (
	brokers              string
	group                string
	kafkaDebug           bool
	queryTimeout         time.Duration
	topicRefreshInterval time.Duration
	prevTopics           []string

	stop int64 // Access atomically, non-zero means stop.
)

func main() {
	flag.StringVar(&brokers, "kafka.brokers", "", "Kafka broker addresses. Default derived from hostname.")
	flag.StringVar(&group, "kafka.group", "", "Group name")
	flag.BoolVar(&kafkaDebug, "kafka.debug", false, `Set to true to turn on librdkafka's debugging`)
	flag.DurationVar(&queryTimeout, "query.timeout", 5*time.Second, "Timeout when querying topic metadata")
	flag.DurationVar(&topicRefreshInterval, "topic.refresh.interval", 30*time.Second, "Interval at which to check for new topics")
	flag.Parse()

	if group == "" {
		group = fmt.Sprintf("kafka-logger.%v", time.Now().Nanosecond())
	}

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGPIPE)
	go func() {
		<-ch
		atomic.StoreInt64(&stop, 1)
		<-ch
		// A second CTRL-C: exit quickly without bothering to close the consumer.
		os.Exit(0)
	}()

	for {
		err := logKafka()
		if err == nil {
			break
		}
		log.Printf("Could not query broker: %s\n", err)
		time.Sleep(time.Second)
	}
}

func logKafka() error {
	brokers = strings.TrimSpace(brokers)
	if brokers == "" {
		brokers = os.Getenv("BROKERS")
	}
	if brokers == "" {
		brokers = "localhost:9092"
	}

	log.Printf("kcat for brokers %v", brokers)

	config := kafka.ConfigMap{
		"bootstrap.servers":               brokers,
		"client.id":                       fmt.Sprintf("%s-%d", "kcat", os.Getpid()),
		"group.id":                        group,
		"go.events.channel.enable":        false,
		"socket.keepalive.enable":         true,
		"enable.auto.commit":              true,
		"auto.commit.interval.ms":         1000,
		"auto.offset.reset":               "latest",
		"broker.address.family":           "v4",
	}

	if kafkaDebug {
		config["debug"] = "all"
	}

	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return err
	}

	defer func() {
		consumer.Close()
	}()

	lastSubscribe := time.Now()
	err = subscribeAll(consumer)
	if err != nil {
		return err
	}

	for atomic.LoadInt64(&stop) == 0 {
		if ev := consumer.Poll(500); ev != nil {
			switch ev := ev.(type) {
			case kafka.Error:
				log.Printf("Kafka error: %s\n", ev) // Carry on, though, underlying library will retry.
			case *kafka.Message:
				write(ev)
			case kafka.AssignedPartitions, kafka.RevokedPartitions:
				log.Printf("%T: %s\n", ev)
			case *kafka.Stats, kafka.OffsetsCommitted, kafka.PartitionEOF, kafka.Stats:
			// Ignore these.

			default:
				log.Printf("Received [%T] %s\n", ev, ev)
			}
		}

		if topicRefreshInterval > 0 && time.Since(lastSubscribe) > topicRefreshInterval {
			err = subscribeAll(consumer)
			if err != nil {
				return err
			}
			lastSubscribe = time.Now()
		}
	}

	return nil
}

func subscribeAll(consumer *kafka.Consumer) error {
	timeout := queryTimeout.Nanoseconds() / int64(time.Millisecond)
	md, err := consumer.GetMetadata(nil, true, int(timeout))
	if err != nil {
		return err
	}

	var topics []string
	for name := range md.Topics {
		topics = append(topics, name)
	}

	topics = filterTopics(topics)

	if strings.Join(prevTopics, ",") != strings.Join(topics, ",") {
		log.Println("Subscribing to", topics)
		prevTopics = topics
	}

	return consumer.SubscribeTopics(topics, nil)
}

func filterTopics(topics []string) []string {
	t := make([]string, 0, len(topics))

	for _, topic := range topics {
		if !strings.HasPrefix(topic, "__") {
			t = append(t, topic)
		}
	}

	sort.Strings(t)
	return t
}

func write(msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic

	if len(msg.Value) == 0 {
		return
	}

	timestamp := time.Now().UTC().Format(RFC3339Milli)

	fmt.Printf("\"%s\\t%s %#q\"\n", topic, timestamp, msg.Value)
}
