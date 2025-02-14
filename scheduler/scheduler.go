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

func (s *Scheduler) DoTask(wid int, chs chan *meta.TaskMeta, results chan *Result, dones chan struct{}) {
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
		go s.DoTask(i, chs, results, dones)
	}
	go func() {
		for i := 0; i < s.Cmdline.Concurrency; i++ {
			<-dones
		}
		close(results)

	}()
	for result := range results {
		logger.Infof("taskid:%d result is %v", result.taskId, result)
	}
	return nil
}

//并发执行的
func (s *Scheduler) worker(ctx context.Context, wid int, tasks chan *TaskParams, resultChan chan *Result, finishedChan chan int, dones chan *workerResult, tm *meta.TaskMeta, reader,writer Plugin) {
	executed := 0
	for p := range tasks {
		task := &Task{
			taskParam: p,
			wid:       wid,
			status:    0,
		}
		
		resultChan <- reader.ExecuteTask(ctx, wid, task, finishedChan, tm, writer)
		executed += 1
	}
	dones <- &workerResult{
		wid:      wid,
		executed: executed,
	}
}
/*
命令行传了一堆task_id 这里负责执行单个的task
*/
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
	Splits := reader.SplitTaskParams(ctx,tm)
	totalTask := len(Splits)
	tasks := make(chan *TaskParams, 0)
	finishedChan := make(chan int, 0)
	resultChan := make(chan *Result, 0)
	dones := make(chan *workerResult, tm.WorkerNum)
	go func() {
		for index, param := range Splits {
			param.index = index
			tasks <- param
		}
		close(tasks)
	}()
	//打印进度协程
	go func() {
		finished := 0
		for range finishedChan {
			finished += 1
			logger.Infof("[finished process is %d/%d,unfinished is %d/%d] \n", finished, totalTask, totalTask-finished, totalTask)
		}
	}()
	for wid := 0; wid < tm.WorkerNum; wid++ {
		go s.worker(ctx, wid, tasks, resultChan, finishedChan, dones, tm, writer)
	}
	go func() {
		for wid := 0; wid < tm.WorkerNum; wid++ {
			w := <-dones
			logger.Infof("workerid %d executed:%d \n ", w.wid, w.executed)
		}
		close(finishedChan)
		close(resultChan)
	}()

	totalSyncNum := int64(0)
	for r := range resultChan {
		logger.Infof("taskIndex:%d (start:%d:end:%d),wid:%d,syncNum:%d,status:%d \n", r.taskParam.index, r.taskParam.start, r.taskParam.end, r.wid, r.syncNum, r.status)
		totalSyncNum += r.syncNum
	}
	logger.Infof("from mysql reader data totalSyncNum %d success", totalSyncNum)
	
	
	writerPlugin, err := plugin.GetPlugin(tm.ToDbType)
	if err != nil {
		return err
	}
	writerConnKey := fmt.Sprintf("to.%s.%s_%s", tm.ToDbType, tm.ToApp, tm.ToDb)
	writer, err := writerPlugin.Connect(s.config, writerConnKey)
	if err != nil {
		return err
	}
	defer func(){
		reader.Close()
		writer.Close()
	}()
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
