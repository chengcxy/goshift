package scheduler

import (
	"context"
	"fmt"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/job"
	"github.com/chengcxy/goshift/logger"
	"github.com/chengcxy/goshift/plugin"
	"time"
)

type Scheduler struct {
	Config       *configor.Config
	GlobalConfig *configor.Config
	StartTime    time.Time
	Cmdline      *Cmdline
	taskClient   TaskDBClient
	ctx          context.Context
	cancelFunc   context.CancelFunc
}

type workerResult struct {
	wid          int
	executedJobs int
}

// 并发执行的
func (s *Scheduler) worker(ctx context.Context, wid int, jobParamsChan chan *job.JobParam, resultChan chan *job.JobResult, finishedChan chan int, dones chan *workerResult, tm *job.TaskMeta, reader plugin.Reader, writer plugin.Writer) {
	executedJobs := 0
	for jobParam := range jobParamsChan {
		job := &job.Job{
			Param:  jobParam,
			Wid:    wid,
			Status: 0,
		}
		resultChan <- reader.Read(ctx, wid, job, finishedChan, tm, writer)
		executedJobs += 1
	}
	dones <- &workerResult{
		wid:          wid,
		executedJobs: executedJobs,
	}
}

func (s *Scheduler) Run() error {
	tm, err := s.taskClient.LoadTaskFromDB(s.Cmdline.TaskId)
	s.taskClient.Close()
	if err != nil {
		logger.Errorf("get task info error %s", s.Cmdline.TaskId)
		return err
	}
	tm.Mode = s.Cmdline.Mode
	logger.Infof("run taskMeta is %v \n ", tm)
	reader, err := plugin.GetReader(tm.FromDbType)
	if err != nil {
		logger.Errorf("Reader = %s not support", tm.FromDbType)
		return err
	}
	readerConnKey := fmt.Sprintf("from.%s.%s_%s", tm.FromDbType, tm.FromApp, tm.FromDb)
	readerConfig, ok := s.GlobalConfig.Get(readerConnKey)
	if !ok {
		logger.Errorf("ReaderCnfig = %s not in db_global json config file", readerConnKey)
		return fmt.Errorf("ReaderCnfig not in db_global json config file")
	}

	err = reader.Connect(readerConfig.(map[string]interface{}))
	if err != nil {
		logger.Errorf("Reader connect failed error:%v", err)
		return err
	}
	writer, err := plugin.GetWriter(tm.ToDbType)
	if err != nil {
		logger.Errorf("Writer = %s not support", tm.ToDbType)
		return err
	}
	writerConnKey := fmt.Sprintf("to.%s.%s_%s", tm.ToDbType, tm.ToApp, tm.ToDb)
	writerConfig, ok := s.GlobalConfig.Get(writerConnKey)
	if !ok {
		logger.Errorf("writerConfig = %s not in db_global json config file", writerConnKey)
		return fmt.Errorf("writerConfig not in db_global json config file")
	}
	err = writer.Connect(writerConfig.(map[string]interface{}))
	if err != nil {
		logger.Errorf("writer connect failed:%v", err)
		return err
	}
	//所有的切分任务
	JobParamsSplits := reader.SplitJobParams(s.ctx, tm)
	if len(JobParamsSplits) < tm.WorkerNum {
		tm.WorkerNum = len(JobParamsSplits)
	}
	totalJobNum := len(JobParamsSplits)
	jobParamsChan := make(chan *job.JobParam, 0)
	finishedChan := make(chan int, 0)
	resultChan := make(chan *job.JobResult, 0)
	dones := make(chan *workerResult, tm.WorkerNum)
	go func() {
		for index, jobParam := range JobParamsSplits {
			jobParam.Index = index
			jobParamsChan <- jobParam
		}
		close(jobParamsChan)
	}()
	//打印进度协程
	go func() {
		finished := 0
		for range finishedChan {
			finished += 1
			logger.Infof("[finished process is %d/%d,unfinished is %d/%d] \n", finished, totalJobNum, totalJobNum-finished, totalJobNum)
		}
	}()

	for wid := 0; wid < tm.WorkerNum; wid++ {
		go s.worker(s.ctx, wid, jobParamsChan, resultChan, finishedChan, dones, tm, reader, writer)
	}
	go func() {
		for wid := 0; wid < tm.WorkerNum; wid++ {
			w := <-dones
			logger.Infof("workerid %d executedJobs:%d \n ", w.wid, w.executedJobs)
		}
		close(finishedChan)
		close(resultChan)
	}()

	totalSyncNum := int64(0)
	for r := range resultChan {
		logger.Infof("taskIndex:%d (start:%d:end:%d),wid:%d,syncNum:%d,status:%d \n", r.Param.Index, r.Param.Start, r.Param.End, r.Wid, r.SyncNum, r.Status)
		totalSyncNum += r.SyncNum
	}
	logger.Infof("from [%s.%s.%s]%s reader sync to [%s.%s.%s] %s writer  totalSyncNum %d ", tm.FromApp, tm.FromDb, tm.FromTable, tm.FromDbType, tm.ToApp, tm.ToDb, tm.ToTable, tm.ToDbType, totalSyncNum)
	reader.Close()
	writer.Close()
	return nil
}

func NewScheduler(ctx context.Context, config, globalConfig *configor.Config, startTime time.Time, Cmdline *Cmdline) (*Scheduler, error) {
	s := &Scheduler{
		Config:       config,
		GlobalConfig: globalConfig,
		StartTime:    startTime,
		Cmdline:      Cmdline,
		ctx:          ctx,
	}
	conf, _ := s.Config.Get("task_meta")
	taskclient, err := NewMySQLTaskDBClient(conf.(map[string]interface{}))
	if err != nil {
		logger.Errorf("get taskDbclient error %v", err)
		return nil, err
	}
	s.taskClient = taskclient
	return s, nil
}
