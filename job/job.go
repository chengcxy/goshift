package job

// 定义状态常量
const (
	StatusSubmitted = iota + 1 // 1 提交
	StatusRunning              // 2 运行中
	StatusCancelled            // 3 取消
	StatusSuccess              // 4 成功
	StatusFailed               // 5 失败
)

type TaskMeta struct {
	Id           int64                  `json:"id"`            // 统计任务id
	FromApp      string                 `json:"from_app"`      // 统计来源业务系统
	FromDbType   string                 `json:"from_db_type"`  // 读取的数据源类型
	FromDb       string                 `json:"from_db"`       // 来自数据库
	FromTable    string                 `json:"from_table"`    // 来自数据库
	ToApp        string                 `json:"to_app"`        // 写入的业务系统
	ToDbType     string                 `json:"to_db_type"`    // 写入数据源类型
	ToDb         string                 `json:"to_db"`         // 写入数据库
	ToTable      string                 `json:"to_table"`      // 写入数据表
	OnlineStatus int64                  `json:"online_status"` // 统计状态0统计1不统计
	TaskDesc     string                 `json:"task_desc"`     // 任务描述
	TaskType     int64                  `json:"task_type"`     // 任务类型1=source2base同步;2=表到表无转换同步
	Owner        string                 `json:"owner"`         // 通知人
	SrcPk        string                 `json:"src_pk"`        // 原表主键
	DestPk       string                 `json:"dest_pk"`       // 目标表主键
	ReadBatch    int                    `json:"read_batch"`    //读取batch
	WriteBatch   int                    `json:"write_batch"`   //读取batch
	WorkerNum    int                    `json:"worker_num"`    //读取batch
	Params       map[string]interface{} `json:"params"`        //params
	Mode         string                 `json:"mode"`          //params
}



type JobParam struct {
	Index int
	Start int64
	End   int64
}

type JobResult struct {
	Param   *JobParam
	Wid     int
	Status  int
	SyncNum int64
	Err     error
}

type Job struct {
	Param  *JobParam
	Wid    int
	Status int
}
