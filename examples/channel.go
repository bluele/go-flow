package main

import (
	"log"
	"time"

	"github.com/bluele/go-flow/flow"
)

func main() {
	in := flow.NewTask(
		"input",
		flow.WithOutputs(flow.NewChannelOutput("channel", make(chan interface{}, 1))),
		flow.WithProcessor(func(tk flow.Task) error {
			for i := 0; i < 10; i++ {
				tk.Out().Write(i)
			}
			return nil
		}),
	)
	out := flow.NewTask(
		"output",
		flow.WithInputs(in.Out()),
		flow.WithProcessor(func(tk flow.Task) error {
			for it := range tk.In().Channel() {
				time.Sleep(time.Second)
				log.Println(it.(int))
			}
			return nil
		}),
		flow.WithWorker(3),
	)
	_, err := flow.Run(out)
	if err != nil {
		panic(err)
	}
}
