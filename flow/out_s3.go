package flow

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Output struct {
	bucket     string
	path       string
	client     *s3.S3
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader

	srz *Serializer
	mu  sync.RWMutex

	closed chan struct{} // writer closed channel
	f      *os.File
	buf    chan interface{}
	isSkip bool
}

// pathToLocalはs3 keyをlocal pathに変換します
func pathToLocal(dir, path string) string {
	if path[0] == '/' {
		path = path[1:]
	}
	lpath := filepath.Join(dir, path)
	return lpath
}

func NewS3Output(c client.ConfigProvider, bucket, path string, srz *Serializer) (*S3Output, error) {
	if srz == nil {
		srz = DefaultSerializer
	}
	out := &S3Output{
		client:     s3.New(c),
		downloader: s3manager.NewDownloader(c),
		uploader:   s3manager.NewUploader(c),
		bucket:     bucket,
		path:       path,
		srz:        srz,
		closed:     make(chan struct{}),
	}
	if out.isSkip = out.isS3FileExists(); !out.isSkip {
		f, err := ioutil.TempFile("", filepath.Base(path))
		if err != nil {
			return nil, err
		}
		out.f = f
	}
	return out, nil
}

func (out *S3Output) Read() (interface{}, error) {
	return <-out.Channel(), nil
}

func (out *S3Output) Channel() chan interface{} {
	out.mu.Lock()
	defer out.mu.Unlock()
	if out.buf != nil {
		return out.buf
	}
	out.buf = make(chan interface{})
	go func() {
		wbuf := aws.NewWriteAtBuffer([]byte{})
		params := &s3.GetObjectInput{
			Bucket: aws.String(out.bucket),
			Key:    aws.String(out.path),
		}
		_, err := out.downloader.Download(wbuf, params, func(dw *s3manager.Downloader) {
			dw.Concurrency = 5
		})
		if err != nil {
			panic(err)
		}
		lines := bytes.Split(wbuf.Bytes(), []byte{'\n'})
		lineNum := len(lines)
		for i, line := range lines {
			v, err := out.srz.Deserialize(line)
			if err != nil {
				Logger.Printf("file %v, deserialize error: %v\n", out.path, err)
				continue
			}
			if i == lineNum-1 && len(line) == 0 {
				break
			}
			out.buf <- v
		}
		close(out.buf)
	}()
	return out.buf
}

func (out *S3Output) Write(v interface{}) error {
	b, err := out.srz.Serialize(v)
	if err != nil {
		return err
	}
	out.mu.Lock()
	defer out.mu.Unlock()
	_, err = out.f.Write(append(b, '\n'))
	return err
}

// isS3FileExists returns true if
func (out *S3Output) isS3FileExists() bool {
	input := &s3.GetObjectInput{
		Bucket: aws.String(out.bucket),
		Key:    aws.String(out.path),
	}
	_, err := out.client.GetObject(input)
	if err != nil {
		return false
	}
	return true
}

func (out *S3Output) Close() error {
	defer close(out.closed)
	if out.f != nil {
		if err := out.commit(); err != nil {
			return err
		}
		return out.f.Close()
	}
	return nil
}

func (out *S3Output) commit() error {
	f, err := os.Open(out.f.Name())
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = out.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(out.bucket),
		Key:    aws.String(out.path),
		Body:   f,
	})
	if err != nil {
		return err
	}
	return nil
}

func (out *S3Output) IsSkip() bool {
	return out.isSkip
}

func (out *S3Output) Ready() chan struct{} {
	return out.closed
}

func (out *S3Output) Destroy() {
	defer close(out.closed)
	if out.f != nil {
		out.f.Close()
	}
}

func (out *S3Output) String() string {
	return fmt.Sprintf("s3://%v%v (%T)", out.bucket, out.path, out)
}
