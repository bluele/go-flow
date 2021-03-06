package flow

import (
	"errors"
	"reflect"
)

type Input interface {
	Channel() chan interface{}
	Ready() chan struct{}
	Read() (interface{}, error)
	String() string
}

type EmptyInput struct{}

func (in *EmptyInput) Ready() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (in *EmptyInput) Read() (interface{}, error) {
	return nil, errors.New("no more lines")
}

func (in *EmptyInput) Channel() chan interface{} {
	ch := make(chan interface{})
	close(ch)
	return ch
}

func (in *EmptyInput) String() string {
	return "empty"
}

// CombineInputs combines multiple inputs into single input
func CombineInputs(ins ...Input) Input {
	if len(ins) == 0 {
		return new(EmptyInput)
	}
	tasks := []*task{}
	cases := make([]reflect.SelectCase, len(ins))
	for i, in := range ins {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(in.Channel()),
		}
		tasks = append(tasks, in.(TaskInput).Tasks()...)
	}
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		for {
			i, v, ok := reflect.Select(cases)
			if ok {
				ch <- v.Interface()
			} else {
				newCases := []reflect.SelectCase{}
				for j, cs := range cases {
					if j != i {
						newCases = append(newCases, cs)
					}
				}
				if len(newCases) == 0 {
					break
				}
				cases = newCases
			}
		}
	}()
	return &combinedTaskInput{
		tks:    tasks,
		inputs: ins,
		Output: &ChannelOutput{
			ch:   ch,
			name: "combined-inputs",
		},
	}
}

type combinedTaskInput struct {
	tks    []*task
	inputs []Input
	Output
}

func (to *combinedTaskInput) Tasks() []*task {
	return to.tks
}

func resolveDependentInputs(in Input) []Input {
	ins := []Input{}
	switch input := in.(type) {
	case *taskInput:
		if input.Output != nil {
			ins = append(ins, input)
		}
	case *combinedTaskInput:
		for _, in := range input.inputs {
			ins = append(ins, resolveDependentInputs(in)...)
		}
	}
	return ins
}
