package plugin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/chengcxy/goshift/job"
	"github.com/chengcxy/goshift/logger"
	_ "github.com/go-sql-driver/mysql"
)

var BaseQuery = `
select *
from %s.%s
where %s>? and %s<=?
`

var BaseQueryMinMax = `
select min(%s)-1 as minId,max(%s) as maxId
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

type MysqlReader struct {
	client *sql.DB
}

func (m *MysqlReader) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return m.client.ExecContext(ctx, query, args...)
}

func (m *MysqlReader) Read(ctx context.Context, wid int, j *job.Job, finishedChan chan int, tm *job.TaskMeta, writer Writer) *job.JobResult {
	defer func() {
		finishedChan <- 1
	}()
	start, end := j.Param.Start, j.Param.End
	logger.Infof("ExecuteTask start is %d,end is %d", start, end)
	query := fmt.Sprintf(BaseQuery, tm.FromDb, tm.FromTable, tm.SrcPk, tm.SrcPk)

	datas, _, err := m.QueryContext(ctx, query, start, end)
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

func (m *MysqlReader) Write(ctx context.Context, wid int, j *job.Job, datas []map[string]interface{}, tm *job.TaskMeta) *job.JobResult {
	db := tm.ToDb
	table := tm.ToTable
	writeBatch := tm.WriteBatch // 每次批量写入的大小
	item := datas[0]
	columns := make([]string, 0)
	for k := range item {
		columns = append(columns, k)
	}
	syncNum := int64(0)
	columnNames := strings.Join(columns, ", ")
	for len(datas) > 0 {
		end := writeBatch
		if end > len(datas) {
			end = len(datas)
		}
		batch := datas[:end]
		placeholders := make([]string, len(batch))
		values := make([]interface{}, 0)
		for j, row := range batch {
			valuePlaceholders := make([]string, len(columns))
			for k, col := range columns {
				valuePlaceholders[k] = "?"
				values = append(values, row[col])
			}
			placeholders[j] = fmt.Sprintf("(%s)", strings.Join(valuePlaceholders, ", "))
		}
		query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s", db, table, columnNames, strings.Join(placeholders, ", "))
		_, err := m.client.ExecContext(ctx, query, values...)
		if err != nil {
			logger.Errorf("Failed to insert batch: %v", err)
			result := &job.JobResult{
				Param:  j.Param,
				Wid:    wid,
				Status: job.StatusFailed,
				Err:    err,
			}
			return result
		}

		// 输出日志，成功插入
		logger.Infof("Successfully inserted %d records into table %s", len(batch), table)
		// 将处理过的部分数据从 datas 中切割出去，释放内存
		datas = datas[end:]
		syncNum += int64(len(batch))
	}
	return &job.JobResult{
		Param:   j.Param,
		Wid:     wid,
		Status:  job.StatusSuccess,
		Err:     nil,
		SyncNum: syncNum,
	}

}

func (m *MysqlReader) SplitJobParams(ctx context.Context, tm *job.TaskMeta) (Splits []*job.JobParam) {
	var minId,maxId int64
	query := fmt.Sprintf(BaseQueryMinMax, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable)
	logger.Infof("MysqlReader.SplitJobParams.query %s", query)
	logger.Infof("MysqlReader.client %v", m.client)
	err := m.client.QueryRowContext(ctx, query).Scan(&minId, &maxId)
	if err != nil {    
		logger.Errorf("get min max error %v", err)
		return nil
	}
	logger.Infof("minId,maxId ->(%d,%d]", minId, maxId)
	// //全量模式 读src表的最小最大id 增量模式 最小取min(src,dest),最大取max(src_dest)
	// if tm.Mode == "init" {
	// 	minId = int64(_minId) - 1
	// 	maxId = int64(_maxId)
	// } else {
	// 	//暂时先不考虑增量
	// 	minId = int64(_minId) - 1
	// 	maxId = int64(_maxId)
	// }

	start := minId
	end := maxId
	logger.Infof("minid:%d,maxId:%d", minId, maxId)
	for start < end {
		q := fmt.Sprintf(BaseGetNextPk, tm.SrcPk, tm.SrcPk, tm.FromDb, tm.FromTable, tm.SrcPk, start, tm.ReadBatch, tm.SrcPk)
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
		Splits = append(Splits, &job.JobParam{Start: start, End: nextId})
		start = nextId
	}
	return
}

func (m *MysqlReader) Connect(config map[string]interface{}) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		config["user"].(string),
		config["password"].(string),
		config["host"].(string),
		int(config["port"].(float64)),
		config["db"].(string),
		config["charset"].(string),
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.New(fmt.Sprintf("open mysql error:%v", err))
	}
	err = db.Ping()
	if err != nil {
		return errors.New(fmt.Sprintf("ping mysql error:%v", err))
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
	m.client = db
	logger.Infof("connect %s mysql success",config)
	return nil
}

func (m *MysqlReader) QueryContext(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, []string, error) {
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
		results = append(results, record)
	}
	return results, columns, nil
}

func (m *MysqlReader) ExecuteContext(ctx context.Context, sql string, args ...interface{}) (int64, error) {
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

func (m *MysqlReader) Close() {
	m.client.Close()
}

func NewMysqlReader() Reader {
	return &MysqlReader{}
}

func NewMysqlWriter() Writer {
	return &MysqlReader{}
}
