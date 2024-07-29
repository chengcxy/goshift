package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/logger"
	"github.com/chengcxy/goshift/meta"
	"github.com/chengcxy/goshift/plugin"
	"sync"
	"time"
)

type Scheduler struct {
	config     *configor.Config
	StartTime  time.Time
	Cmdline    *Cmdline
	taskClient plugin.Plugin
	Tasks      []*meta.TaskMeta
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (s *Scheduler) getTasksinfo() error {
	defer s.taskClient.Close()
	var query string
	if s.Cmdline.Cmd == "sync" {
		query = fmt.Sprintf(BaseQueryTaskMeta, fmt.Sprintf(" id = %s ", s.Cmdline.TaskId))
	} else {
		query = fmt.Sprintf(BaseQueryTaskMeta, " 1 = 1")
	}
	metas, columns, err := s.taskClient.(plugin.MysqlPlugin).QueryContext(s.ctx, query)
	if err != nil {
		panic(err)
	}
	if len(metas) < s.Cmdline.Concurrency {
		s.Cmdline.Concurrency = len(metas)
	}
	for _, task := range metas {
		fromDbType := string(task["from_db_type"].([]byte))
		fromApp := string(task["from_app"].([]byte))
		fromDb := string(task["from_db"].([]byte))
		toDbType := string(task["to_db_type"].([]byte))
		toApp := string(task["to_app"].([]byte))
		toDb := string(task["to_db"].([]byte))
		params := string(task["params"].([]uint8))
		var p map[string]interface{}
		json.Unmarshal([]byte(params), &p)
		task["params"] = p
		pkMap := p["pk"].(map[string]interface{})
		srcPk := pkMap["src"].(string)
		destPk := pkMap["dest"].(string)
		workerNum := int(p["worker_num"].(float64))
		readBatch := int(p["read_batch"].(float64))
		writeBatch := int(p["write_batch"].(float64))
		tm := &meta.TaskMeta{
			Id:           task["id"].(int64),
			FromApp:      fromApp,
			FromDbType:   fromDbType,
			FromDb:       fromDb,
			FromTable:    string(task["from_table"].([]byte)),
			ToApp:        toApp,
			ToDbType:     toDbType,
			ToDb:         toDb,
			ToTable:      string(task["to_table"].([]byte)),
			OnlineStatus: task["online_status"].(int64),
			SrcPk:        srcPk,
			DestPk:       destPk,
			ReadBatch:    readBatch,
			WriteBatch:   writeBatch,
			WorkerNum:    workerNum,
			Params:       p,
			Mode:         s.Cmdline.Mode,
		}
		s.Tasks = append(s.Tasks, tm)
	}
	logger.Infof("get tasks %d columns:%+v", len(s.Tasks), columns)
	return nil
}

func NewWorkerPool(WorkerNum int) *WorkerPool {
	return &WorkerPool{
		WorkerNum:  WorkerNum,
		JobChan:    make(chan JobInterface, 0),
		ResultChan: make(chan interface{}, 0),
		wg:         sync.WaitGroup{},
	}
}

func (s *Scheduler) worker(wid int, chs chan *meta.TaskMeta, results chan *Result, dones chan struct{}) {
	defer func() {
		dones <- struct{}{}
	}()
	for task := range chs {
		err := s.run(task)
		r := &Result{
			taskId: task.Id,
			err:    err,
		}
		results <- r
	}
}

func (s *Scheduler) Run() error {

	//判断是否ctx Done
	s.getTasksinfo()
	chs := make(chan *meta.TaskMeta, 0)
	results := make(chan *Result, 0)
	dones := make(chan struct{}, s.Cmdline.Concurrency)
	go func() {
		defer func() {
			close(chs)
		}()
		for _, task := range s.Tasks {
			chs <- task
		}
	}()
	//worker execute task
	for i := 0; i < s.Cmdline.Concurrency; i++ {
		go s.worker(i, chs, results, dones)
	}
	go func() {
		for i := 0; i < s.Cmdline.Concurrency; i++ {
			<-dones
		}
		close(results)

	}()
	for result := range results {
		logger.Infof("taskid:%d result is %s", result.taskId, result)
	}
	return nil
}

func (s *Scheduler) run(tm *meta.TaskMeta) error {
	fmt.Printf("run taskMeta is %v \n ", tm)
	readerPlugin, err := plugin.GetPlugin(tm.FromDbType)
	if err != nil {
		return err
	}
	readerConnKey := fmt.Sprintf("from.%s.%s_%s", tm.FromDbType, tm.FromApp, tm.FromDb)
	reader, err := readerPlugin.Connect(s.config, readerConnKey)
	if err != nil {
		return err
	}
	defer reader.Close()
	writerPlugin, err := plugin.GetPlugin(tm.ToDbType)
	if err != nil {
		return err
	}
	writerConnKey := fmt.Sprintf("to.%s.%s_%s", tm.ToDbType, tm.ToApp, tm.ToDb)
	writer, err := writerPlugin.Connect(s.config, writerConnKey)
	if err != nil {
		return err
	}
	defer writer.Close()
	return reader.Read(s.ctx, writer, tm)
}

func NewScheduler(config *configor.Config, startTime time.Time, Cmdline *Cmdline, ctx context.Context) (*Scheduler, error) {
	s := &Scheduler{
		config:    config,
		StartTime: startTime,
		Cmdline:   Cmdline,
		ctx:       ctx,
	}
	p, _ := plugin.GetPlugin("mysql")
	taskclient, err := p.Connect(s.config, "task_meta_mysql")
	if err != nil {
		return nil, err
	}
	s.taskClient = taskclient
	return s, nil
}
