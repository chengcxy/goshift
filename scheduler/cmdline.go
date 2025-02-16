package scheduler

type Cmdline struct {
	Cmd    string
	TaskId string
	Mode   string
}

func NewCmdline(cmd, taskId, mode string) *Cmdline {
	return &Cmdline{
		Cmd:    cmd,
		TaskId: taskId,
		Mode:   mode,
	}
}
