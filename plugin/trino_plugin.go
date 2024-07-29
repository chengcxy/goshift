package plugin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/logger"
	"github.com/chengcxy/goshift/meta"
	"github.com/chengcxy/goshift/utils"
	_ "github.com/trinodb/trino-go-client/trino"
	"strings"
)

var BaseQueryMinMaxPartion = `
select min(%s) as minId,max(%s) as maxId
from %s.%s
where add_partion_date=?
`
var BaseQueryPartition = `
select *
from %s.%s
where add_partion_date=? and %s>? and %s<=?
`

var BaseGetNextPkPartition = `
select a.%s as nextId
from (
	select %s
	from %s.%s
	where add_partion_date=? and %s > %d
	limit %d
) as a
order by %s desc
limit 1
`

type TrinoPlugin struct {
	MysqlPlugin
}

func (tp TrinoPlugin) Connect(config *configor.Config, key string) (Plugin, error) {
	tp.config = config
	conf, ok := config.Get(key)
	if !ok {
		return nil, errors.New(fmt.Sprintf("key:%s not in json_file", key))
	}
	m := conf.(map[string]interface{})
	dsn := fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s",
		m["user"].(string),
		m["host"].(string),
		int(m["port"].(float64)),
		m["catalog"].(string),
		m["schema"].(string),
	)
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}
	tp.client = db
	return &TrinoPlugin{
		MysqlPlugin: MysqlPlugin{config: tp.config, client: tp.client},
	}, err
}

func (tp TrinoPlugin) GetTotalSplits(ctx context.Context, tm *meta.TaskMeta) (Splits []*TaskParams, err error) {
	var minId, maxId int64
	q := fmt.Sprintf(BaseQueryMinMaxPartion, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable)
	err = tp.client.QueryRowContext(ctx, q, utils.GetYestory()).Scan(&minId, &maxId)
	if err != nil {
		logger.Errorf("get min max error %v", err)
		return
	}
	minId = minId - 1
	logger.Infof("minid:%d,maxId:%d", minId, maxId)
	start := minId
	end := maxId
	for start < end {
		q := fmt.Sprintf(BaseGetNextPkPartition, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable, tm.SrcPk, start, tm.ReadBatch, tm.SrcPk)
		logger.Infof("query next pk is \n %s", q)
		var nextId int64
		err := tp.client.QueryRowContext(ctx, q, utils.GetYestory()).Scan(&nextId)
		if err != nil {
			logger.Errorf("nextId err is %v", err)
			break
		} else {
			logger.Infof("nextId is %d", nextId)
		}
		if nextId >= end {
			nextId = end
		}
		Splits = append(Splits, &TaskParams{start: start, end: nextId})
		start = nextId
	}
	return
}

func (tp TrinoPlugin) worker(ctx context.Context, wid int, tasks chan *TaskParams, resultChan chan *Result, finishedChan chan int, dones chan *workerResult, tm *meta.TaskMeta, writer Plugin) {
	executed := 0
	for p := range tasks {
		task := &Task{
			taskParam: p,
			wid:       wid,
			status:    0,
		}
		resultChan <- tp.ExecuteTask(ctx, wid, task, finishedChan, tm, writer)
		executed += 1
	}
	dones <- &workerResult{
		wid:      wid,
		executed: executed,
	}
}
func (tp TrinoPlugin) ExecuteTask(ctx context.Context, wid int, task *Task, finishedChan chan int, tm *meta.TaskMeta, writer Plugin) *Result {
	start, end := task.taskParam.start, task.taskParam.end
	logger.Infof("ExecuteTask start is %d,end is %d", start, end)
	q := fmt.Sprintf(BaseQueryPartition, tm.FromDb, tm.FromTable, tm.SrcPk, tm.SrcPk)
	rows, _ := tp.client.Query(q, utils.GetYestory(), start, end)
	defer rows.Close()
	columns, _ := rows.Columns()
	insertKeys := make([]string, len(columns))
	qs := make([]string, 0)
	for index, col := range columns {
		columns[index] = strings.ToLower(col)
		insertKeys[index] = fmt.Sprintf("`%s`", strings.ToLower(col))
		qs = append(qs, "?")
	}
	values := make([]interface{}, 0)
	scanArgs := make([]interface{}, len(columns))
	rowValues := make([]interface{}, len(columns))
	for i := range rowValues {
		scanArgs[i] = &rowValues[i]
	}
	fmts := make([]string, 0)
	syncNum := int64(0)
	for rows.Next() {
		rows.Scan(scanArgs...)
		fmts = append(fmts, fmt.Sprintf("(%s)", strings.Join(qs, ",")))
		values = append(values, rowValues...)
		if len(values) == tm.WriteBatch*len(columns) {
			insertSql := fmt.Sprintf(BaseInsertSql, tm.ToDb, tm.ToTable, strings.Join(insertKeys, ","), strings.Join(fmts, ","))
			r, _ := writer.ExecContext(ctx, insertSql, values...)
			num, _ := r.RowsAffected()
			syncNum += num
			values = values[:0]
			fmts = fmts[:0]
		}

	}
	if len(values) > 0 {
		insertSql := fmt.Sprintf(BaseInsertSql, tm.ToDb, tm.ToTable, strings.Join(insertKeys, ","), strings.Join(fmts, ","))
		r, err := writer.ExecContext(ctx, insertSql, values...)
		if err != nil {
			logger.Errorf("insertsql %s error:%v", insertSql, err)
		}
		num, _ := r.RowsAffected()
		syncNum += num
		values = values[:0]
		fmts = fmts[:0]
	}

	finishedChan <- 1
	return &Result{
		taskParam: task.taskParam,
		wid:       wid,
		status:    1,
		syncNum:   syncNum,
	}

}

func (tp TrinoPlugin) Read(ctx context.Context, writer Plugin, tm *meta.TaskMeta) error {
	Splits, err := tp.GetTotalSplits(ctx, tm)
	if err != nil {
		logger.Errorf("split min max error:%v", err)
		return err
	}
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
			logger.Infof("[finished process is %d/%d,unfinished is %d/%d]", finished, totalTask, totalTask-finished, totalTask)
		}
	}()
	for wid := 0; wid < tm.WorkerNum; wid++ {
		go tp.worker(ctx, wid, tasks, resultChan, finishedChan, dones, tm, writer)
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
	logger.Infof("from trino reader data totalSyncNum %d success", totalSyncNum)
	return nil
}

func NewTrinoPlugin() Plugin {
	return TrinoPlugin{}
}

func init() {
	RegisterPlugin("trino", NewTrinoPlugin())
}
