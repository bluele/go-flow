package flow

import "sync"

type Task interface {
	// Name returns task name
	Name() string
	// In returns a reader that can accept the output of other task
	In(...int) Input
	// Out returns a writer that can input to other task
	Out(...int) Output
	// Requres returns the task list on which this task depends
	Requires() []Task

	init() error
	run() error
	skip() error
	isDone() bool
	// this channel returns a value when all inputs is ready
	ready() chan struct{}
	destroy()
}

type Input interface {
	Channel() chan interface{}
	Ready() chan struct{}
	Read() (interface{}, error)
}

type task struct {
	name string

	processor func(Task) error
	requires  []Task

	inputs  []Input
	outputs []Output

	workerNumber int
	wg           sync.WaitGroup

	once sync.Once
}

func (tk *task) init() error {
	for i := 0; i < tk.workerNumber; i++ {
		tk.wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			tk.processor(tk)
		}(&tk.wg)
	}
	return nil
}

func (tk *task) In(idx ...int) Input {
	if len(idx) == 0 {
		return tk.inputs[0]
	}
	return tk.inputs[idx[0]]
}

func (tk *task) Out(idx ...int) Output {
	if len(idx) == 0 {
		return tk.outputs[0]
	}
	return tk.outputs[idx[0]]
}

func (tk *task) destroy() {
	for _, out := range tk.outputs {
		out.Destroy()
	}
}

func (tk *task) isDone() bool {
	if len(tk.outputs) == 0 {
		return false
	}
	for _, out := range tk.outputs {
		if !out.IsDone() {
			return false
		}
	}
	return true
}

func (tk *task) ready() chan struct{} {
	ch := make(chan struct{})
	wg := new(sync.WaitGroup)
	for _, in := range tk.inputs {
		wg.Add(1)
		go func(in Input) {
			defer wg.Done()
			<-in.Ready()
		}(in)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func (tk *task) skip() error {
	for _, out := range tk.outputs {
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (tk *task) run() error {
	var err error
	tk.once.Do(func() {
		if err = tk.init(); err != nil {
			return
		}
		for _, out := range tk.outputs {
			defer out.Close()
		}
		tk.wg.Wait()
	})
	return err
}

func (tk *task) Name() string {
	return tk.name
}

func (tk *task) Requires() []Task {
	return tk.requires
}

type options struct {
	InitFunc     func() error
	Inputs       []Input
	Outputs      []Output
	Processor    func(Task) error
	WorkerNumber int
}

type Options func(*options)

func defaultOptions() *options {
	return &options{
		WorkerNumber: 1,
	}
}

func WithInputs(ins ...Input) Options {
	return func(opts *options) {
		opts.Inputs = append(opts.Inputs, ins...)
	}
}

func WithOutputs(outs ...Output) Options {
	return func(opts *options) {
		opts.Outputs = append(opts.Outputs, outs...)
	}
}

func WithProcessor(processor func(Task) error) Options {
	return func(opts *options) {
		opts.Processor = processor
	}
}

func WithWorker(workerNumber int) Options {
	return func(opts *options) {
		if workerNumber <= 0 {
			workerNumber = 1
		}
		opts.WorkerNumber = workerNumber
	}
}

// NewTask returns a new task with specified input, output, processor
func NewTask(name string, opts ...Options) Task {
	op := defaultOptions()
	for _, opt := range opts {
		opt(op)
	}
	tk := &task{
		name:         name,
		processor:    op.Processor,
		inputs:       op.Inputs,
		workerNumber: op.WorkerNumber,
	}
	for _, out := range op.Outputs {
		tk.outputs = append(tk.outputs, &taskOutput{
			tk:     tk,
			Output: out,
		})
	}
	var requires []Task
	for _, in := range op.Inputs {
		t := in.(*taskOutput).tk
		// TODO use a better logic
		found := false
		for _, req := range requires {
			if req.Name() == t.Name() {
				found = true
				break
			}
		}
		if !found {
			requires = append(requires, t)
		}
	}
	tk.requires = requires
	return tk
}
