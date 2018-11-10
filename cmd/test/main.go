package main

import (
	"log"
	"net/http"

	"github.com/joho/godotenv"

	"github.com/shjp/shjp-core"
	"github.com/shjp/shjp-queue"
)

func main() {
	envVars, err := godotenv.Read()
	if err != nil {
		panic(err)
	}

	host := envVars["HOST"]
	user := envVars["USER"]

	// Init
	c, err := queue.NewClient(host, user)
	abortOnError(err)

	abortOnError(c.Register("test", "model-req", "*.request.model.#"))
	abortOnError(c.Register("test", "model-res", "*.success.model.#"))
	abortOnError(c.Register("test", "model-res", "*.failure.model.#"))

	// Start listening
	consumer, err := queue.NewConsumer(host, user, "queue-test", core.IntentRequest)
	if err != nil {
		panic(err)
	}

	if err = consumer.Consume(true, handleMessage); err != nil {
		log.Println("starting to consume")
		panic(err)
	}

	// Publish
	producer, err := queue.NewProducer(host, user)
	if err != nil {
		panic(err)
	}

	msg := modelRequest("id124", "group", `{"foo": "bar"}`)
	if err = producer.Publish(string(core.IntentRequest), msg); err != nil {
		panic(err)
	}

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		log.Println("sending msg")
		err := producer.Publish("test", core.NewMessage(
			"pingid",
			core.IntentRequest,
			core.ModelType,
			"ping",
			core.UnknownOperation,
			nil,
			nil))
		if err != nil {
			panic(err)
		}
	})
	log.Println("")

	log.Println("Test server listening to port 5001")
	log.Fatal(http.ListenAndServe(":5001", nil))
	/*forever := make(chan bool)
	<-forever*/
}

func handleMessage(msg *core.Message) {
	log.Printf("Received message || key: %s || type: %s || subtype: %s || intent: %s || operation: %s || data: %s\n", msg.Key, msg.Type, msg.Subtype, msg.Intent, msg.OperationType, string(msg.Data))
}

func modelRequest(id, subtype, data string) *core.Message {
	msg := core.NewMessage(id, core.IntentRequest, core.ModelType, "group", core.CreateOperation, []byte(data), nil)
	return msg
}

func abortOnError(err error) {
	if err != nil {
		panic(err)
	}
}
