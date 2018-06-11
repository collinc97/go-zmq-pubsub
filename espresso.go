package main

import (
	"math/rand"
	"time"

	zmq "github.com/pebbe/zmq4"

	"fmt"
)

// subscriber thread - requests messages starting with
// A and B then reads and counts incoming messages

func subscriber_thread() {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	subscriber.Connect("tcp://localhost:6001")
	subscriber.SetSubscribe("A")
	subscriber.SetSubscribe("B")
	defer subscriber.Close()

	for count := 0; count < 5; count++ {
		_, err := subscriber.RecvMessage(0)
		if err != nil {
			fmt.Println("subscriber error")
			break
		}
	}
}

// publisher thread - sends random messages starting with A-J

func publisher_thread() {
	publisher, _ := zmq.NewSocket(zmq.PUB)
	publisher.Bind("tcp://*:6000")

	for {
		s := fmt.Sprintf("%c-%05d", rand.Intn(10)+'A', rand.Intn(100000))
		_, err := publisher.SendMessage(s)
		if err != nil {
			fmt.Println("publisher error")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// listening thread - receeives all messages flowing through the proxy, on its
// pipe. Here, the pipe is a pair of ZMQ_PAIR sockets that connects attached
// child thread via inproc.
func listener_thread() {
	pipe, _ := zmq.NewSocket(zmq.PAIR)
	pipe.Bind("inproc://pipe")

	for {
		msg, err := pipe.RecvMessage(0)
		if err != nil {
			fmt.Println("listener error")
			break
		}
		fmt.Printf("%q\n", msg)
	}
}

func main() {

	fmt.Println("Initializing theads")

	go publisher_thread()
	go subscriber_thread()
	go listener_thread()

	time.Sleep(100 * time.Millisecond)

	fmt.Println("Connecting threads")

	subscriber, _ := zmq.NewSocket(zmq.XSUB)
	subscriber.Connect("tcp://localhost:6000")
	publisher, _ := zmq.NewSocket(zmq.XPUB)
	publisher.Bind("tcp://*:6001")
	listener, _ := zmq.NewSocket(zmq.PAIR)
	listener.Connect("inproc://pipe")
	zmq.Proxy(subscriber, publisher, listener)

	fmt.Println("interrupted")
}
