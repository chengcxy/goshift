package plugin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/chengcxy/goshift/job"
	"github.com/chengcxy/goshift/logger"
	"github.com/chengcxy/goshift/utils"
	_ "github.com/trinodb/trino-go-client/trino"
)

var BaseQueryTrino = `
select *
from %s.%s
where %s>? and %s<=? and %s
`

var BaseQueryTrinoMinMax = `
select min(%s)-1 as minId,max(%s) as maxId
from %s.%s
where %s
`


var BaseGetTrinoNextPk = `
select a.%s as nextId
from (
	select %s
	from %s.%s
	where %s > %d and %s
	limit %d
) as a
order by %s desc
limit 1
`

type TrinoReader struct {
	MysqlReader
	addWhere string
	partionDate string
}


func (t *TrinoReader) SplitJobParams(ctx context.Context, tm *job.TaskMeta) (Splits []*job.JobParam) {
	var minId,maxId int64
	partion := tm.Params["partion"].(map[string]interface{})
	partionColumn := partion["column"].(string)
	partionDateCondition := partion["partion_date"].(string)
	partionDate,err := utils.GetTimeValue(partionDateCondition)
	if err != nil{
		logger.Infof("utils.GetTimeValue(partionDateCondition=%s) error:%v",partionDateCondition,err)
		return
	}
	t.partionDate = partionDate
	t.addWhere = fmt.Sprintf("%s=?",partionColumn)
	query := fmt.Sprintf(BaseQueryTrinoMinMax, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable,t.addWhere)
	logger.Infof("TrinoReader.SplitJobParams.query %s", query)
	err = t.client.QueryRowContext(ctx, query,t.partionDate).Scan(&minId, &maxId)
	if err != nil {
		logger.Errorf("get min max error %v", err)
		return nil
	}
	logger.Infof("minId,maxId ->(%d,%d]", minId, maxId)
	start := minId
	end := maxId
	logger.Infof("minid:%d,maxId:%d", minId, maxId)
	for start < end {
		q := fmt.Sprintf(BaseGetTrinoNextPk, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable, tm.SrcPk, start,t.addWhere,tm.ReadBatch, tm.SrcPk)
		logger.Infof("query next pk is \n %s", q)
		var nextId int64
		err := t.client.QueryRowContext(ctx, q,t.partionDate).Scan(&nextId)
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
		Splits = append(Splits, &job.JobParam{Start: start, End: nextId})
		start = nextId
	}
	return
}


func (t *TrinoReader) Read(ctx context.Context, wid int, j *job.Job, finishedChan chan int, tm *job.TaskMeta, writer Writer) *job.JobResult {
	defer func() {
		finishedChan <- 1
	}()
	start, end := j.Param.Start, j.Param.End
	logger.Infof("ExecuteTask start is %d,end is %d", start, end)
	query := fmt.Sprintf(BaseQueryTrino, tm.FromDb, tm.FromTable, tm.SrcPk, tm.SrcPk,t.addWhere)
	datas, _, err := t.QueryContext(ctx, query, start, end,t.partionDate)
	if err != nil {
		return &job.JobResult{
			Param:  j.Param,
			Wid:    wid,
			Status: job.StatusFailed,
			Err:    err,
		}
	}
	return writer.Write(ctx, wid, j, datas, tm)
}

func (t *TrinoReader) Connect(config map[string]interface{}) error {
	dsn := fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s",
		config["user"].(string),
		config["host"].(string),
		int(config["port"].(float64)),
		config["catalog"].(string),
		config["schema"].(string),
	)
	db, err := sql.Open("trino", dsn)
	if err != nil {
		logger.Errorf("TrinoReader.Connect.sql.open trino failed %v",err)
		return errors.New(fmt.Sprintf("open trino error:%v", err))
	}
	err = db.Ping()
	if err != nil {
		logger.Errorf("TrinoReader.Connect.ping trino failed %v",err)
		return errors.New(fmt.Sprintf("ping trino error:%v", err))
	}
	db.SetConnMaxLifetime(0)
	MaxOpenConns, ok := config["MaxOpenConns"]
	if ok {
		db.SetMaxOpenConns(int(MaxOpenConns.(float64)))
	} else {
		db.SetMaxOpenConns(20)
	}
	MaxIdleConns, ok := config["MaxIdleConns"]
	if ok {
		db.SetMaxIdleConns(int(MaxIdleConns.(float64)))
	} else {
		db.SetMaxIdleConns(20)
	}
	t.client = db
	logger.Infof("TrinoReader.Connect trino success")
	return nil
}

func (t *TrinoReader) Close() {
	t.client.Close()
}

func NewTrinoReader() Reader {
	return &TrinoReader{}
}
