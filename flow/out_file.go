package flow

import (
	"errors"
	"io"
	"os"
	"sync"
)

// streaming I/O
type FileStreaming struct {
	path        string
	w           *os.File
	t           *tail
	buf         chan interface{}
	isDone      bool
	serialize   SerializeFunc
	deserialize DeserializeFunc
	mu          sync.RWMutex
}

type (
	SerializeFunc   func(interface{}) ([]byte, error)
	DeserializeFunc func([]byte) (interface{}, error)
)

func NewFileStreaming(path string, serialize SerializeFunc, deserialize DeserializeFunc) (*FileStreaming, error) {
	var (
		w   *os.File
		err error
	)
	isDone := IsFileExists(path)
	if !isDone {
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
	return &FileStreaming{
		path:        path,
		w:           w,
		t:           t,
		serialize:   serialize,
		deserialize: deserialize,
		isDone:      isDone,
	}, nil
}

func (fs *FileStreaming) Write(v interface{}) error {
	if fs.isDone {
		return errors.New("cannot write to closed stream")
	}
	b, err := fs.serialize(v)
	if err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, err = fs.w.Write(b)
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
	return fs.deserialize([]byte(line.Text))
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
			if b, err := fs.deserialize(line.Text); err != nil {
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

func (fs *FileStreaming) IsDone() bool {
	return fs.isDone
}

func (fs *FileStreaming) Ready() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

type FileBuffer struct {
	path        string
	w           *os.File      // writer
	closed      chan struct{} // writer closed channel
	t           *tail
	buf         chan interface{} // reader channel
	isDone      bool
	serialize   SerializeFunc
	deserialize DeserializeFunc
	mu          sync.RWMutex
}

func NewFileBuffer(path string, serialize SerializeFunc, deserialize DeserializeFunc) (*FileBuffer, error) {
	var (
		w   *os.File
		err error
	)
	isDone := IsFileExists(path)
	if !isDone {
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
	return &FileBuffer{
		path:        path,
		w:           w,
		t:           t,
		serialize:   serialize,
		deserialize: deserialize,
		isDone:      isDone,
		closed:      make(chan struct{}),
	}, nil
}

func (fb *FileBuffer) Write(v interface{}) error {
	if fb.isDone {
		return errors.New("cannot write to closed stream")
	}
	b, err := fb.serialize(v)
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
	return fb.deserialize([]byte(line.Text))
}

func (fb *FileBuffer) Channel() chan interface{} {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	if fb.buf != nil {
		return fb.buf
	}
	buf := make(chan interface{})
	go func() {
		for line := range fb.t.Lines {
			if line.Error == io.EOF {
				Logger.Printf("closed %v\n", fb.path)
				close(buf)
				return
			} else if line.Error != nil {
				Logger.Printf("file %v, occurred err: %v\n", fb.path, line.Error)
				continue
			}
			if b, err := fb.deserialize(line.Text); err != nil {
				Logger.Printf("file %v, deserialize error: %v\n", fb.path, err)
				return
			} else {
				buf <- b
			}
		}
	}()
	fb.buf = buf
	return buf
}

func (fb *FileBuffer) IsDone() bool {
	return fb.isDone
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
