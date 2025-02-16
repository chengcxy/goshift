package scheduler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/chengcxy/goshift/job"
)

var BaseQueryTaskMeta = `
SELECT from_db_type, from_app, from_db,from_table, to_db_type, to_app, to_db,to_table,params
FROM task_def_sync_manager
WHERE id = %s
`

// TaskDBClient 定义任务数据库客户端接口
type TaskDBClient interface {
	LoadTaskFromDB(taskID string) (*job.TaskMeta, error)
	Close()
}

// MySQLTaskDBClient 实现 TaskDBClient 接口
type MySQLTaskDBClient struct {
	db *sql.DB
}

// NewMySQLTaskDBClient 创建 MySQL 任务数据库客户端
func NewMySQLTaskDBClient(config map[string]interface{}) (*MySQLTaskDBClient, error) {
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
		return nil, fmt.Errorf("failed to connect to task database: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping task database: %v", err)
	}

	return &MySQLTaskDBClient{db: db}, nil
}

// LoadTaskFromDB 从任务数据库加载任务配置
func (m *MySQLTaskDBClient) LoadTaskFromDB(taskID string) (*job.TaskMeta, error) {
	query := fmt.Sprintf(BaseQueryTaskMeta, taskID)
	row := m.db.QueryRow(query)
	var tm job.TaskMeta
	var params string
	var err error
	if err = row.Scan(&tm.FromDbType, &tm.FromApp, &tm.FromDb, &tm.FromTable, &tm.ToDbType, &tm.ToApp, &tm.ToDb, &tm.ToTable, &params); err != nil {
		return nil, fmt.Errorf("failed to load task: %v", err)
	}
	var conf map[string]interface{}
	err = json.Unmarshal([]byte(params), &conf)
	if err != nil {
		return nil, err
	}
	tm.Params = conf
	pkMap := tm.Params["pk"].(map[string]interface{})
	srcPk := pkMap["src"].(string)
	destPk := pkMap["dest"].(string)
	workerNum := int(tm.Params["worker_num"].(float64))
	readBatch := int(tm.Params["read_batch"].(float64))
	writeBatch := int(tm.Params["write_batch"].(float64))
	tm.SrcPk = srcPk
	tm.DestPk = destPk
	tm.ReadBatch = readBatch
	tm.WriteBatch = writeBatch
	tm.WorkerNum = workerNum
	return &tm, nil
}

func (m *MySQLTaskDBClient) Close() {
	m.db.Close()
}
