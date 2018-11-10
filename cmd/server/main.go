package main

import (
	"encoding/json"
	"io/ioutil"
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

	p, err := queue.NewProducer(host, user)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/model", handle(p, core.ModelType))
	http.HandleFunc("/storage", handle(p, core.FileType))

	log.Println("Server listening on port 5000")
	log.Fatal(http.ListenAndServe(":5000", nil))
}

func handle(p *queue.Producer, messageType core.MessageType) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			p.Publish("main", core.TypedFailureMessage(messageType, nil, err))
			return
		}

		var msg core.Message
		if err = json.Unmarshal(body, &msg); err != nil {
			p.Publish("main", core.TypedFailureMessage(messageType, body, err))
			return
		}

		p.Publish("main", &msg)
	}
}
