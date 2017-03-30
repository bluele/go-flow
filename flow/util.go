package flow

import (
	"bufio"
	"io"
	"log"
	"os"
	"time"
)

var (
	// If you don't want to print debug logs, please disable the output of this Logger
	Logger = log.New(os.Stdout, "", log.LstdFlags)
	// Polling interval when EOF is reached
	TailPollInterval = 250 * time.Millisecond
)

type Line struct {
	Text  []byte
	Error error
}

type tail struct {
	r     io.ReadCloser
	br    *bufio.Reader
	Lines chan *Line
	done  chan bool
}

func newTail(r io.ReadCloser) *tail {
	t := &tail{
		r:     r,
		br:    bufio.NewReader(r),
		Lines: make(chan *Line),
		done:  make(chan bool),
	}
	return t
}

func (t *tail) Run() {
	poll := time.NewTicker(TailPollInterval)
	defer poll.Stop()
	for {
		line, _, err := t.br.ReadLine()
		if err == io.EOF {
			select {
			case <-t.done:
				line, _, err = t.br.ReadLine()
				if err == io.EOF {
					t.Lines <- &Line{Text: line, Error: err}
					return
				}
			case <-poll.C:
				continue
			}
		}
		t.Lines <- &Line{Text: line, Error: err}
	}
}

func (t *tail) Stop() {
	close(t.done)
}

func IsFileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}
