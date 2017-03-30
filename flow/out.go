package flow

type Output interface {
	Write(interface{}) error

	Close() error
	Destroy()

	Channel() chan interface{}
	Read() (interface{}, error)
	IsDone() bool
	Ready() chan struct{}
}

type taskOutput struct {
	tk *task
	Output
}

type ChannelOutput struct {
	ch chan interface{}
}

func NewChannelOutput(ch chan interface{}) *ChannelOutput {
	return &ChannelOutput{
		ch: ch,
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

func (co *ChannelOutput) IsDone() bool {
	return false
}

func (co *ChannelOutput) Ready() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}