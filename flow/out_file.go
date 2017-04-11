package flow

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	ErrSerializeValue = errors.New("ErrSerializeValue")
	DefaultSerializer = &Serializer{
		Serialize:   defaultSerialize,
		Deserialize: defaultDeserialize,
	}
)

type (
	SerializeFunc   func(interface{}) ([]byte, error)
	DeserializeFunc func([]byte) (interface{}, error)
)

type Serializer struct {
	Serialize   SerializeFunc
	Deserialize DeserializeFunc
}

func defaultSerialize(iv interface{}) ([]byte, error) {
	switch v := iv.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, ErrSerializeValue
	}
}

func defaultDeserialize(b []byte) (interface{}, error) {
	return b, nil
}

// streaming I/O
type FileStreaming struct {
	path   string
	w      *os.File
	t      *tail
	buf    chan interface{}
	isSkip bool
	srz    *Serializer
	mu     sync.RWMutex
}

func NewFileStreaming(path string, srz *Serializer) (*FileStreaming, error) {
	var (
		w   *os.File
		err error
	)
	isSkip := IsFileExists(path)
	if !isSkip {
		if w, err = os.Create(path); err != nil {
			return nil, err
		}
	}
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t := newTail(r)
	go t.Run()

	if srz == nil {
		srz = DefaultSerializer
	}
	return &FileStreaming{
		path:   path,
		w:      w,
		t:      t,
		srz:    srz,
		isSkip: isSkip,
	}, nil
}

func (fs *FileStreaming) Write(v interface{}) error {
	if fs.isSkip {
		return errors.New("cannot write to closed stream")
	}
	b, err := fs.srz.Serialize(v)
	if err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, err = fs.w.Write(append(b, '\n'))
	return err
}

func (fs *FileStreaming) Read() (interface{}, error) {
	line := <-fs.t.Lines
	if line == nil {
		return nil, io.EOF
	}
	if line.Error != nil {
		if line.Error == io.EOF {
			close(fs.buf)
		}
		return nil, line.Error
	}
	return fs.srz.Deserialize([]byte(line.Text))
}

func (fs *FileStreaming) Channel() chan interface{} {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.buf != nil {
		return fs.buf
	}
	buf := make(chan interface{})
	go func() {
		for line := range fs.t.Lines {
			if line.Error == io.EOF {
				Logger.Printf("closed %v\n", fs.path)
				close(buf)
				return
			} else if line.Error != nil {
				Logger.Printf("file %v, occurred err: %v\n", fs.path, line.Error)
				continue
			}
			if b, err := fs.srz.Deserialize(line.Text); err != nil {
				Logger.Printf("file: %v, deserialize error: %v\n", fs.path, err)
				return
			} else {
				buf <- b
			}
		}
	}()
	fs.buf = buf
	return buf
}

func (fs *FileStreaming) Close() error {
	defer fs.t.Stop()
	if fs.w != nil {
		return fs.w.Close()
	}
	return nil
}

func (fs *FileStreaming) Destroy() {
	fs.Close()
	os.Remove(fs.path)
}

func (fs *FileStreaming) IsSkip() bool {
	return fs.isSkip
}

func (fs *FileStreaming) Ready() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (fs *FileStreaming) String() string {
	return fmt.Sprintf("%v(%T)", fs.path, fs)
}

type FileBuffer struct {
	path   string
	w      *os.File      // writer
	closed chan struct{} // writer closed channel
	t      *tail
	buf    chan interface{} // reader channel
	isSkip bool
	srz    *Serializer
	mu     sync.RWMutex
}

func NewFileBuffer(path string, srz *Serializer) (*FileBuffer, error) {
	var (
		w   *os.File
		err error
	)
	isSkip := IsFileExists(path)
	if !isSkip {
		if w, err = os.Create(path); err != nil {
			return nil, err
		}
	}
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t := newTail(r)
	go t.Run()
	if srz == nil {
		srz = DefaultSerializer
	}
	return &FileBuffer{
		path:   path,
		w:      w,
		t:      t,
		srz:    srz,
		isSkip: isSkip,
		closed: make(chan struct{}),
	}, nil
}

func (fb *FileBuffer) Write(v interface{}) error {
	if fb.isSkip {
		return errors.New("cannot write to closed stream")
	}
	b, err := fb.srz.Serialize(v)
	if err != nil {
		return err
	}
	fb.mu.Lock()
	defer fb.mu.Unlock()
	_, err = fb.w.Write(b)
	return err
}

func (fb *FileBuffer) Read() (interface{}, error) {
	line := <-fb.t.Lines
	if line == nil {
		return nil, io.EOF
	}
	if line.Error != nil {
		if line.Error == io.EOF {
			close(fb.buf)
		}
		return nil, line.Error
	}
	return fb.srz.Deserialize([]byte(line.Text))
}

func (fb *FileBuffer) Channel() chan interface{} {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	if fb.buf != nil {
		return fb.buf
	}
	fb.buf = make(chan interface{})
	go func() {
		for line := range fb.t.Lines {
			if line.Error == io.EOF {
				Logger.Printf("closed %v\n", fb.path)
				close(fb.buf)
				return
			} else if line.Error != nil {
				Logger.Printf("file %v, occurred err: %v\n", fb.path, line.Error)
				continue
			}
			if b, err := fb.srz.Deserialize(line.Text); err != nil {
				Logger.Printf("file %v, deserialize error: %v\n", fb.path, err)
				return
			} else {
				fb.buf <- b
			}
		}
	}()
	return fb.buf
}

func (fb *FileBuffer) IsSkip() bool {
	return fb.isSkip
}

func (fb *FileBuffer) Close() error {
	defer close(fb.closed)
	defer fb.t.Stop()
	if fb.w != nil {
		return fb.w.Close()
	}
	return nil
}

func (fb *FileBuffer) Ready() chan struct{} {
	return fb.closed
}

func (fb *FileBuffer) Destroy() {
	fb.Close()
	os.Remove(fb.path)
}

func (fb *FileBuffer) String() string {
	return fmt.Sprintf("%v(%T)", fb.path, fb)
}
