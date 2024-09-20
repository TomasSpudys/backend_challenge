package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sync"
	"time"
)

type Message struct {
	ID      int
	Topic   string
	Content string
}

type Topic struct {
	Name        string
	Subscribers map[chan Message]bool
	Mutex       sync.Mutex
}

var topics = make(map[string]*Topic)
var topicsMutex = sync.Mutex{}
var globalMessageID = 0
var globalMutex = sync.Mutex{}

const maxConnectionTime = 30 * time.Second

func getOrCreateTopic(name string) *Topic {
	topicsMutex.Lock()
	defer topicsMutex.Unlock()
	if _, ok := topics[name]; !ok {
		topics[name] = &Topic{
			Name:        name,
			Subscribers: make(map[chan Message]bool),
		}
	}
	return topics[name]
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]

	messageContent := make([]byte, r.ContentLength)
	r.Body.Read(messageContent)
	r.Body.Close()

	globalMutex.Lock()
	globalMessageID++
	messageID := globalMessageID
	globalMutex.Unlock()

	topic := getOrCreateTopic(topicName)

	message := Message{
		ID:      messageID,
		Topic:   topicName,
		Content: string(messageContent),
	}

	topic.Mutex.Lock()
	for subscriber := range topic.Subscribers {
		subscriber <- message
	}
	topic.Mutex.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

func receiveMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topic"]

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	topic := getOrCreateTopic(topicName)

	messageChan := make(chan Message)
	topic.Mutex.Lock()
	topic.Subscribers[messageChan] = true
	topic.Mutex.Unlock()

	connectedAt := time.Now()
	defer func() {
		// Send timeout event
		fmt.Fprintf(w, "event: timeout\ndata: %ds\n\n", int(time.Since(connectedAt).Seconds()))

		topic.Mutex.Lock()
		delete(topic.Subscribers, messageChan)
		close(messageChan)
		topic.Mutex.Unlock()
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case message := <-messageChan:
			fmt.Fprintf(w, "id: %d\nevent: msg\ndata: %s\n\n", message.ID, message.Content)
			flusher.Flush()
		case <-time.After(maxConnectionTime):
			return
		}
	}
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/infocenter/{topic}", sendMessage).Methods("POST")
	r.HandleFunc("/infocenter/{topic}", receiveMessages).Methods("GET")

	fmt.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
