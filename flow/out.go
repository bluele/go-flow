package flow

import "fmt"

// Output is output interface
type Output interface {
	Write(interface{}) error

	Close() error
	Destroy()

	Channel() chan interface{}
	Read() (interface{}, error)
	IsSkip() bool
	Ready() chan struct{}

	String() string
}

type taskOutput struct {
	tk *task
	Output
}

type ChannelOutput struct {
	ch   chan interface{}
	name string
}

func NewChannelOutput(name string, ch chan interface{}) *ChannelOutput {
	return &ChannelOutput{
		ch:   ch,
		name: name,
	}
}

func (co *ChannelOutput) Write(v interface{}) error {
	co.ch <- v
	return nil
}

func (co *ChannelOutput) Read() (interface{}, error) {
	return <-co.ch, nil
}

func (co *ChannelOutput) Channel() chan interface{} {
	return co.ch
}

func (co *ChannelOutput) Close() error {
	close(co.ch)
	return nil
}

func (co *ChannelOutput) Destroy() {}

func (co *ChannelOutput) IsSkip() bool {
	return false
}

func (co *ChannelOutput) Ready() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (co *ChannelOutput) String() string {
	return fmt.Sprintf("%v(%T)", co.name, co)
}
