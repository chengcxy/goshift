package plugin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/logger"
	"github.com/chengcxy/goshift/meta"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

var BaseQuery = `
select *
from %s.%s
where %s>? and %s<=?
`

var BaseQueryMinMax = `
select min(%s) as minId,max(%s) as maxId
from %s.%s
`
var BaseInsertSql = `
INSERT INTO %s.%s (%s) VALUES %s
`

var BaseGetNextPk = `
select a.%s as nextId
from (
	select %s
	from %s.%s
	where %s > %d
	limit %d
) as a
order by %s desc
limit 1
`

type MysqlPlugin struct {
	config *configor.Config
	client *sql.DB
}

type Params struct {
}
type TaskParams struct {
	index int
	start int64
	end   int64
}
type Task struct {
	taskParam *TaskParams
	wid       int
	status    int
}

type Result struct {
	taskParam *TaskParams
	wid       int
	status    int
	syncNum   int64
}

type workerResult struct {
	wid      int
	executed int
}

func (m MysqlPlugin) Exec(query string, args ...interface{}) (sql.Result, error) {
	return m.client.Exec(query, args...)
}

func (m MysqlPlugin) ExecuteTask(wid int, task *Task, finishedChan chan int, tm *meta.TaskMeta, writer Plugin) *Result {
	start, end := task.taskParam.start, task.taskParam.end
	logger.Infof("ExecuteTask start is %d,end is %d", start, end)
	q := fmt.Sprintf(BaseQuery, tm.FromDb, tm.FromTable, tm.SrcPk, tm.SrcPk)
	rows, _ := m.client.Query(q, start, end)
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
			r, err := writer.Exec(insertSql, values...)
			if err != nil {
				logger.Errorf("insertsql:%s error:%v", insertSql, err)
			}
			num, _ := r.RowsAffected()
			syncNum += num
			values = values[:0]
			fmts = fmts[:0]
		}

	}
	if len(values) > 0 {
		insertSql := fmt.Sprintf(BaseInsertSql, tm.ToDb, tm.ToTable, strings.Join(insertKeys, ","), strings.Join(fmts, ","))
		r, err := writer.Exec(insertSql, values...)
		if err != nil {
			logger.Errorf("insertsql:%s error:%v", insertSql, err)
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
func (m MysqlPlugin) worker(wid int, tasks chan *TaskParams, resultChan chan *Result, finishedChan chan int, dones chan *workerResult, tm *meta.TaskMeta, writer Plugin) {
	executed := 0
	for p := range tasks {
		task := &Task{
			taskParam: p,
			wid:       wid,
			status:    0,
		}
		resultChan <- m.ExecuteTask(wid, task, finishedChan, tm, writer)
		executed += 1
	}
	dones <- &workerResult{
		wid:      wid,
		executed: executed,
	}
}
func (m MysqlPlugin) GetTotalSplits(ctx context.Context, tm *meta.TaskMeta, start, end int64, batch int) (Splits []*TaskParams) {
	for start < end {
		q := fmt.Sprintf(BaseGetNextPk, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable, tm.SrcPk, start, batch, tm.SrcPk)
		logger.Infof("query next pk is \n %s", q)

		var nextId int64
		err := m.client.QueryRowContext(ctx, q).Scan(&nextId)
		if err != nil {
			fmt.Errorf("nextId err is %v", err)
			break
		} else {
			logger.Infof("nextId is %d", nextId)
		}
		//_next := start + int64(batch)
		if nextId >= end {
			nextId = end
		}
		Splits = append(Splits, &TaskParams{start: start, end: nextId})
		start = nextId
	}
	return
}

func (m MysqlPlugin) Connect(config *configor.Config, key string) (Plugin, error) {
	m.config = config
	conf, ok := config.Get(key)
	if !ok {
		return nil, errors.New(fmt.Sprintf("key:%s not in json_file", key))
	}
	c := conf.(map[string]interface{})
	Uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		c["user"].(string),
		c["password"].(string),
		c["host"].(string),
		int(c["port"].(float64)),
		c["db"].(string),
		c["charset"].(string),
	)
	db, err := sql.Open("mysql", Uri)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("open mysql error:%v", err))
	}
	err = db.Ping()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("ping mysql error:%v", err))
	}
	db.SetConnMaxLifetime(0)
	MaxOpenConns, ok := c["MaxOpenConns"]
	if ok {
		db.SetMaxOpenConns(int(MaxOpenConns.(float64)))
	} else {
		db.SetMaxOpenConns(20)
	}
	MaxIdleConns, ok := c["MaxIdleConns"]
	if ok {
		db.SetMaxIdleConns(int(MaxIdleConns.(float64)))
	} else {
		db.SetMaxIdleConns(20)
	}
	m.client = db
	return m, nil
}

func (m MysqlPlugin) QueryContext(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, []string, error) {
	stam, err := m.client.PrepareContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	rows, err := stam.Query(args...)
	if err != nil {
		logger.Infof("QueryContext stmt:%s error:%+v", query, err)
		return nil, nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	for index, col := range columns {
		columns[index] = strings.ToLower(col)
	}
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		//将行数据保存到record字典
		err = rows.Scan(scanArgs...)
		record := make(map[string]interface{})
		for i, col := range values {
			record[strings.ToLower(columns[i])] = col
		}
		logger.Infof("record is %+v", record)
		results = append(results, record)
	}
	return results, columns, nil
}

func (m MysqlPlugin) ExecuteContext(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	stam, err := m.client.PrepareContext(ctx, sql)
	defer stam.Close()
	if err != nil {
		logger.Errorf("ExecuteContext PrepareContext error %+v", err)
		return int64(0), err
	}
	result, err := stam.Exec(args...)
	if err != nil {
		logger.Errorf("ExecuteContext stmt:%s error %+v", sql, err)
		return int64(0), err
	}
	affNum, err := result.RowsAffected()
	if err != nil {
		logger.Errorf("ExecuteContext result.RowsAffected error %+v", err)
	}
	return affNum, err
}

func (m MysqlPlugin) Read(ctx context.Context, writer Plugin, tm *meta.TaskMeta) error {
	var minId, maxId int64
	q := fmt.Sprintf(BaseQueryMinMax, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable)
	err := m.client.QueryRowContext(ctx, q).Scan(&minId, &maxId)
	if err != nil {
		logger.Errorf("get min max error %v", err)
		return err
	}
	minId = minId - 1
	logger.Infof("minid:%d,maxId:%d", minId, maxId)
	Splits := m.GetTotalSplits(ctx, tm, minId, maxId, tm.ReadBatch)
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
		go m.worker(wid, tasks, resultChan, finishedChan, dones, tm, writer)
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
	logger.Infof("from mysql reader data write success")
	return nil
}

func (m MysqlPlugin) Close() {
	m.client.Close()
}

func NewMysqlPlugin() Plugin {
	return MysqlPlugin{}
}
