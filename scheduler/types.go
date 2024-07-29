package scheduler

import "sync"

type Cmdline struct {
	Cmd         string
	TaskId      string
	Mode        string
	ToApp       string
	Concurrency int
}

type Result struct {
	taskId int64
	err    error
}

func NewCmdline(cmd, taskId, mode, to_app string, concurrency int) *Cmdline {
	return &Cmdline{
		Cmd:         cmd,
		TaskId:      taskId,
		Mode:        mode,
		ToApp:       to_app,
		Concurrency: concurrency,
	}
}

// 接口
type JobInterface interface {
	Executed() interface{}
}

type WorkerPool struct {
	wg         sync.WaitGroup
	WorkerNum  int
	JobChan    chan JobInterface
	ResultChan chan interface{}
}

var BaseQueryTaskMeta = `
select *
from task_def_sync_manager
where %s
`
