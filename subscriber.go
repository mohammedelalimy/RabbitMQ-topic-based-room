// go run subscriber.go  tanta.*
// go run subscriber.go  *.politics
// go run subscriber.go  egypt.*

package main

import (
	"load_balancer/utils"
	"log"
	"os"
	"strings"
	"github.com/streadway/amqp"
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

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	utils.FailOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		log.Printf("Usage: %s [binding_key]...", os.Args[0])
		os.Exit(0)
	}

	// Display the topics the subscriber is listening to
	log.Printf("========")
	for _, binding := range os.Args[1:] {
		log.Printf("%s", binding)
	}
	log.Printf("========")

	for _, binding := range os.Args[1:] {
		err = ch.QueueBind(
			q.Name,
			binding,
			"logs_topic",
			false,
			nil)
		utils.FailOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	utils.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			topic := d.RoutingKey 
			message := extractMessage(string(d.Body))
			log.Printf("[X] (%s says): %s", topic, message)
}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
// Function to extract only the message content
func extractMessage(body string) string {
	parts := strings.Split(body, ":")
	if len(parts) > 1 {
		messageParts := strings.Split(parts[1], "sent from topic")
		if len(messageParts) > 0 {
			return strings.TrimSpace(messageParts[0])
		}
	}
	return body
}
