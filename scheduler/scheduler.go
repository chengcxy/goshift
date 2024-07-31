package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/logger"
	"github.com/chengcxy/goshift/meta"
	"github.com/chengcxy/goshift/plugin"
	"strconv"
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
	if s.Cmdline.TaskId != "" {
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
		_id := string(task["id"].([]uint8))
		id, _ := strconv.ParseInt(_id, 10, 64)
		_onlineStatus := string(task["online_status"].([]uint8))
		onlineStatus, _ := strconv.ParseInt(_onlineStatus, 10, 64)
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
			Id:           id,
			FromApp:      fromApp,
			FromDbType:   fromDbType,
			FromDb:       fromDb,
			FromTable:    string(task["from_table"].([]byte)),
			ToApp:        toApp,
			ToDbType:     toDbType,
			ToDb:         toDb,
			ToTable:      string(task["to_table"].([]byte)),
			OnlineStatus: onlineStatus,
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

func (s *Scheduler) worker(wid int, chs chan *meta.TaskMeta, results chan *plugin.TaskResult, dones chan struct{}) {
	defer func() {
		dones <- struct{}{}
	}()
	for tm := range chs {
		results <- s.run(tm)
	}
}

func (s *Scheduler) checkPlugin() error {
	for _, tm := range s.Tasks {
		_, err := plugin.GetPlugin(tm.FromDbType)
		if err != nil {
			return err
		}
		_, err = plugin.GetPlugin(tm.ToDbType)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Scheduler) runSync() error {
	chs := make(chan *meta.TaskMeta, 0)
	results := make(chan *plugin.TaskResult, 0)
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
	faileds := make([]*plugin.TaskResult, 0)
	successed := make([]*plugin.TaskResult, 0)
	for result := range results {
		logger.Infof("taskid:%d result %+v", result.Tm.Id, result)
		if result.Err != nil {
			logger.Errorf("taskid:%d failed,error is %v", result.TaskId, result.Err)
			faileds = append(faileds, result)
		} else {
			logger.Infof("taskid:%d success", result.TaskId)
			successed = append(successed, result)
		}
	}
	if len(faileds) > 0 {
		errMsg := "错误为:"
		for _, r := range faileds {
			taskInfo := fmt.Sprintf("task_id:%d-[from_db]:%s-[from_table] %s--> err %v", r.Tm.Id, r.Tm.FromDb, r.Tm.FromTable, r.Err)
			errMsg += taskInfo + "\n"
		}
		logger.Infof("total tasks:%d successed:%d,failed %d", len(s.Tasks), len(successed), len(faileds))
		logger.Infof("task status failed")
		logger.Errorf("%s", errMsg)
		return fmt.Errorf("%s", errMsg)
	}
	logger.Infof("total tasks:%d successed:%d,failed %d", len(s.Tasks), len(successed), len(faileds))
	logger.Infof("task status success")
	return nil
}

func (s *Scheduler) Run() error {
	err := s.getTasksinfo()
	if err != nil {
		logger.Errorf("getTasksinfo err %+v", err)
		return err
	}
	switch s.Cmdline.Cmd {
	case "sync":
		return s.runSync()
	case "checkPlugin":
		return s.checkPlugin()
	default:
		return fmt.Errorf("cmd:%s not support", s.Cmdline.Cmd)
	}
}

func (s *Scheduler) run(tm *meta.TaskMeta) *plugin.TaskResult {
	fmt.Printf("run taskMeta is %v \n ", tm)
	readerPlugin, err := plugin.GetPlugin(tm.FromDbType)
	if err != nil {
		return &plugin.TaskResult{
			TaskId:  tm.Id,
			Err:     err,
			SyncNum: int64(0),
			Tm:      tm,
		}
	}
	readerConnKey := fmt.Sprintf("from.%s.%s_%s", tm.FromDbType, tm.FromApp, tm.FromDb)
	reader, err := readerPlugin.Connect(s.config, readerConnKey)
	if err != nil {
		return &plugin.TaskResult{
			TaskId:  tm.Id,
			Err:     err,
			SyncNum: int64(0),
			Tm:      tm,
		}
	}
	defer reader.Close()
	writerPlugin, err := plugin.GetPlugin(tm.ToDbType)
	if err != nil {
		return &plugin.TaskResult{
			TaskId:  tm.Id,
			Err:     err,
			SyncNum: int64(0),
			Tm:      tm,
		}
	}
	writerConnKey := fmt.Sprintf("to.%s.%s_%s", tm.ToDbType, tm.ToApp, tm.ToDb)
	writer, err := writerPlugin.Connect(s.config, writerConnKey)
	if err != nil {
		return &plugin.TaskResult{
			TaskId:  tm.Id,
			Err:     err,
			SyncNum: int64(0),
			Tm:      tm,
		}
	}
	defer writer.Close()
	r := reader.Read(s.ctx, writer, tm)
	r.Tm = tm
	logger.Infof("read result is %+v", r)
	return r
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
