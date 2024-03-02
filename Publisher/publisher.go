// usage
// go run publisher.go  tanta.politics
// go run publisher.go  egypt.politics
// go run publisher.go  tanta.fun
package main

import (
	"bufio"
	"fmt"
	"github.com/streadway/amqp"
	"load_balancer/utils"
	"os"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	utils.FailOnError(err, "Failed to declare an exchange")

	topic := severityFrom(os.Args)

	for i := 0; ; {
		fmt.Printf("%s> ", topic)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		message := scanner.Text()

		body := fmt.Sprintf("Message %d: %s sent from topic %s", i, message, topic)

		err = ch.Publish(
			"logs_topic",
			topic,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		utils.FailOnError(err, "Failed to publish a message")
		time.Sleep(400 * time.Millisecond)
		i += 1
	}
}
func severityFrom(args []string) string {
	var s string
	if len(args) < 2 || os.Args[1] == "" {
		fmt.Print("Enter topic : ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		s = scanner.Text()
	} else {
		s = os.Args[1]
	}
	return s
}
