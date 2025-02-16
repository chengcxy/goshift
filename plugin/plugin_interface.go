package plugin

import (
	"context"
	"errors"
	"fmt"
	"github.com/chengcxy/goshift/job"
	"sync"
)

type Reader interface {
	Connect(config map[string]interface{}) error
	SplitJobParams(ctx context.Context, tm *job.TaskMeta) []*job.JobParam
	Read(ctx context.Context, wid int, job *job.Job, finishedChan chan int, tm *job.TaskMeta, writer Writer) *job.JobResult
	Close()
}

type Writer interface {
	Connect(config map[string]interface{}) error
	Write(ctx context.Context, wod int, job *job.Job, datas []map[string]interface{}, tm *job.TaskMeta) *job.JobResult
	Close()
}

var Readers = make(map[string]func() Reader, 0)
var Writers = make(map[string]func() Writer, 0)

var lock sync.Mutex

func RegisterReader(readerType string, factory func() Reader) error {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := Readers[readerType]; ok {
		errMsg := fmt.Sprintf("%s reader already Register", readerType)
		return errors.New(errMsg)
	}
	Readers[readerType] = factory
	return nil
}

func GetReader(readerType string) (Reader, error) {
	lock.Lock()
	defer lock.Unlock()
	factory, ok := Readers[readerType]
	if !ok {
		errMsg := fmt.Sprintf("%s plugin not  Register to Readers please user func of init to RegisterPlugin", readerType)
		return nil, errors.New(errMsg)
	}
	return factory(), nil
}

func RegisterWriter(writerType string, factory func() Writer) error {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := Writers[writerType]; ok {
		errMsg := fmt.Sprintf("%s Writer already Register", writerType)
		return errors.New(errMsg)
	}
	Writers[writerType] = factory
	return nil
}

func GetWriter(writerType string) (Writer, error) {
	lock.Lock()
	defer lock.Unlock()
	factory, ok := Writers[writerType]
	if !ok {
		errMsg := fmt.Sprintf("%s plugin not  Register to Writers please user func of init to RegisterPlugin", writerType)
		return nil, errors.New(errMsg)
	}
	return factory(), nil
}

func init() {
	RegisterReader("mysql", func() Reader {
		return NewMysqlReader()
	})
	RegisterReader("trino", func() Reader {
		return NewTrinoReader()
	})
	RegisterWriter("mysql", func() Writer {
		return NewMysqlWriter()
	})
}
