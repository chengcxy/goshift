<div align="center">
<strong>
<samp>

[English](https://github.com/chengcxy/goshift/blob/master/README.md) · [简体中文](https://github.com/chengcxy/goshift/blob/master/README-CN.md)

</samp>
</strong>
</div>


# goshift Data Syncer for Other Databases

## 1. Design Concept


Create a task table in MySQL or other databases.
- `from_db_type` represents the type of the source database.
- `from_app` represents the source system.
- `from_db` represents the source database name.
- `from_table` represents the source table.

Similarly:
- `to_db_type` represents the type of the destination database.
- `to_app` represents the destination system.
- `to_db` represents the destination database.
- `to_table` represents the destination table.

The `params` field contains the parameters. The value of `params` is:
```json
{
  "split": {"pk": ["id"]},
  "worker_num": 20,
  "read_batch": 5000,
  "write_batch": 500
}
```

- The pk under split represents the partition key.
- worker_num is the number of goroutines.
- read_batch is the batch size for reading.
- write_batch is the batch size for writing.

The configuration file follows the JSON format, and the contents look like this:

```json
{
  "from": {
    "mysql": {
      "$from_app_$from_db": {
        "database_connection_details"
      }
    }
  },
  "to": {
    "mysql": {
      "$to_app_$to_db": {
        "database_connection_details"
      }
    }
  }
}
```

When running a task, first, read the database to get a record. According to from_db_type, get the corresponding client. For example, if from_db_type is mysql, the reader is a mysqlclient. The key is composed of from_app + from_db, which can be used to fetch the reader configuration from the JSON file. Similarly, if to_db_type is mysql, you can get the connection information for the mysqlclient.

To support extensibility, the project contains a plugin directory. The plugin_interface.go defines the reader interface. The reader interface has a Connect method to automatically read the config file and create a database connection. Additionally, it has a Read method that automatically partitions and performs concurrent reading according to the params. The writer interface is also defined, which has a Connect method for creating the database connection and a Write method that writes data to the target database in batches.

This project is developed in Go and fully utilizes concurrency for task execution.


Execution Mode

The command-line parameters allow the execution of a single primary key id. After parsing the parameters, it queries the task_id and its configuration from the database. If params sets worker_num = 20, the data synchronization task will start 20 goroutines.

To reduce the pressure on the database's read and write operations and minimize concurrent resource usage, it is recommended not to exceed 20 threads.

Run Method Logic:
- 1.1 Fetch the task_id from command-line parameters, load database data, and assign it to job.TaskMeta pointer.
- 1.2 Get the reader and writer plugins based on from_db_type and to_db_type
- 1.3 Parse the task metadata from the database and determine how many workers to start
- 1.4 Call the reader.SplitTaskParams method to determine how many tasks need to be executed.
- 1.5 Create tasks and result channels. 
- 1.6 The scheduler's worker function listens on the tasks channel, retrieves partition task parameters, and the reader is responsible for reading data based on those parameters. The writer writes data to the target database and updates the progress.
- 1.7 Monitor the result channel and print the synchronization progress.

Future Expansion:
API interface management, asynchronous execution progress monitoring via the front-end page. Since the function encapsulates logic for executing a single task_id, it can be packaged as an Event, generate an asynchronous task ID, and place it into a message queue. A consumer will listen to the queue and update task statuses, forming a closed loop.



## 2. Create Database Table

In the local.json configuration file, create the table in the database specified by the task_meta.db parameter.

```
CREATE TABLE `task_def_sync_manager` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Sync Task ID',
  `from_app` varchar(50) DEFAULT NULL COMMENT 'Source business system',
  `from_db_type` varchar(50) DEFAULT NULL COMMENT 'Source database type',
  `from_db` varchar(20) DEFAULT NULL COMMENT 'Source database name',
  `from_table` varchar(255) DEFAULT '' COMMENT 'Source table',
  `to_app` varchar(50) DEFAULT NULL COMMENT 'Target business system',
  `to_db_type` varchar(20) DEFAULT NULL COMMENT 'Target database type',
  `to_db` varchar(20) DEFAULT NULL COMMENT 'Target database name',
  `to_table` varchar(255) DEFAULT NULL COMMENT 'Target table',
  `params` text COMMENT 'Sync rules',
  `online_status` int(11) DEFAULT NULL COMMENT 'Online status (1: Online, 0: Offline)',
  `task_desc` text COMMENT 'Task description',
  `task_status` varchar(100) DEFAULT NULL COMMENT 'Task status',
  `owner` varchar(100) DEFAULT NULL COMMENT 'Developer',
  `create_time` datetime NOT NULL COMMENT 'Creation time',
  `update_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Sync task management';

INSERT INTO `task_def_sync_manager` (`id`, `from_app`, `from_db_type`, `from_db`, `from_table`, `to_app`, `to_db_type`, `to_db`, `to_table`, `params`, `online_status`, `task_desc`, `task_status`, `owner`, `create_time`, `update_time`)
VALUES
	(5, 'local_dw', 'mysql', 'blog', 'qcc_change_history', 'local_dw', 'mysql', 'blog', 'qcc_change_history2', '{\n	\"pk\": {\n		\"src\": \"id\",\n		\"dest\": \"id\"\n	},\n	\"diff_column\": {\n		\"src\": \"update_time\",\n		\"dest\": \"update_time\"\n	},\n	\"worker_num\": 20,\n	\"read_batch\": 5000,\n	\"write_batch\": 500\n}', 1, '增量导入', '1', '18811788263', '2021-03-30 16:13:34', '2024-07-08 11:00:11');
```


## 3. Assign Environment Variables in Configuration File

```
Check the environment settings for task_meta in the etc directory (taking local environment for Linux or macOS as an example). Set environment variables. The variable names should match those in the configuration file.


export LOCAL_DW_MYSQL_Z_ETL_HOST="localhost"
export LOCAL_DW_MYSQL_Z_ETL_USER="z_etl"
export LOCAL_DW_MYSQL_Z_ETL_PASSWORD="密码自定义"
```

## 4. Build

```bash
//For linux
make linux

//For macOs
make mac
```

## 5.Command Line Parameters


```
➜  cmd git:(master) ✗ ./goshift --help

Usage of ./goshift:
  -UsedEnv
    	Whether to use environment variables (default true)
  -c string
    	Configuration file directory (default "../etc/")
  -cmd string
    	Command (default "sync")
  -e string
    	Running environment - JSON file prefix: dev/test/prod/local (default "local")
  -id string
    	Task ID (default "1")
  -mode string
    	Mode (default "init")

```


## 6.demo

```
./goshift -c ../etc/ -e local -id 5
```

## 7. Log 

```
2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:109	[finished process is 195/200,unfinished is 5/200]

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:127	taskIndex:197 (start:985010:end:990010),wid:6,syncNum:5000,status:4

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:119	workerid 6 executedJobs:10

2025-02-17T22:52:40+08:00	INFO	plugin/mysql_plugin.go:110	Successfully inserted 500 records into table test2
2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:119	workerid 8 executedJobs:10

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:109	[finished process is 196/200,unfinished is 4/200]

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:127	taskIndex:196 (start:980010:end:985010),wid:8,syncNum:5000,status:4

2025-02-17T22:52:40+08:00	INFO	plugin/mysql_plugin.go:110	Successfully inserted 490 records into table test2
2025-02-17T22:52:40+08:00	INFO	plugin/mysql_plugin.go:110	Successfully inserted 500 records into table test2
2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:119	workerid 7 executedJobs:10

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:109	[finished process is 197/200,unfinished is 3/200]

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:127	taskIndex:199 (start:995010:end:1000000),wid:7,syncNum:4990,status:4

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:109	[finished process is 198/200,unfinished is 2/200]

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:119	workerid 10 executedJobs:10

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:127	taskIndex:189 (start:945010:end:950010),wid:10,syncNum:5000,status:4

2025-02-17T22:52:40+08:00	INFO	plugin/mysql_plugin.go:110	Successfully inserted 500 records into table test2
2025-02-17T22:52:40+08:00	INFO	plugin/mysql_plugin.go:110	Successfully inserted 500 records into table test2
2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:119	workerid 17 executedJobs:10

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:109	[finished process is 199/200,unfinished is 1/200]

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:127	taskIndex:195 (start:975010:end:980010),wid:17,syncNum:5000,status:4

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:109	[finished process is 200/200,unfinished is 0/200]

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:127	taskIndex:198 (start:990010:end:995010),wid:4,syncNum:5000,status:4

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:119	workerid 4 executedJobs:10

2025-02-17T22:52:40+08:00	INFO	scheduler/scheduler.go:130	from mysql reader sync2 mysql  totalSyncNum 999990
```

