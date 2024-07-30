# goshift data syncer 2 other database

# Quick Start


## 1.同步任务使用mysql存储的方式，设置同步任务管理客户端的mysql环境变量

```
查看etc目录下某个环境(以local环境linux或mac系统为案例)task_meta_mysql的配置，设置环境变量,变量名自定义和配置文件保持一致即可。
export LOCAL_DW_MYSQL_Z_ETL_HOST="localhost"
export LOCAL_DW_MYSQL_Z_ETL_USER="z_etl"
export LOCAL_DW_MYSQL_Z_ETL_PASSWORD="密码自定义"
```

## 2.创建数据库表
```
在配置文件task_meta_mysql.db参数所在的库建表

CREATE TABLE `task_def_sync_manager` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '统计任务id',
  `from_app` varchar(50) DEFAULT NULL COMMENT '统计来源业务系统',
  `from_db_type` varchar(50) DEFAULT NULL COMMENT '读取的数据源类型',
  `from_db` varchar(20) DEFAULT NULL COMMENT '来自数据库',
  `from_table` varchar(255) DEFAULT '' COMMENT '统计规则',
  `to_app` varchar(50) DEFAULT NULL COMMENT '写入的业务系统',
  `to_db_type` varchar(20) DEFAULT NULL COMMENT '写入数据源类型',
  `to_db` varchar(20) DEFAULT NULL COMMENT '写入数据库',
  `to_table` varchar(255) DEFAULT NULL COMMENT '写入数据表',
  `params` text COMMENT '增量规则',
  `online_status` int(11) DEFAULT NULL COMMENT '在线状态1在线0下线',
  `task_desc` text COMMENT '统计描述',
  `task_status` varchar(100) DEFAULT NULL COMMENT '任务状态',
  `owner` varchar(100) DEFAULT NULL COMMENT '开发人',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='同步任务管理';、


INSERT INTO `task_def_sync_manager` (`id`, `from_app`, `from_db_type`, `from_db`, `from_table`, `to_app`, `to_db_type`, `to_db`, `to_table`, `params`, `online_status`, `task_desc`, `task_status`, `owner`, `create_time`, `update_time`)
VALUES
	(5, 'local_dw', 'mysql', 'blog', 'qcc_change_history', 'local_dw', 'mysql', 'blog', 'qcc_change_history2', '{\n	\"pk\": {\n		\"src\": \"id\",\n		\"dest\": \"id\"\n	},\n	\"diff_column\": {\n		\"src\": \"update_time\",\n		\"dest\": \"update_time\"\n	},\n	\"worker_num\": 20,\n	\"read_batch\": 5000,\n	\"write_batch\": 500\n}', 1, '增量导入', '1', '18811788263', '2021-03-30 16:13:34', '2024-07-08 11:00:11');

```
## 3.数据表数据说明

```
表的主键id代表一个任务id,其中from_db_type代表读取的reader数据库类型，
to_db_type代表写入writer的数据库类型，这个类型会在plugin里注册。
这里以mysql为例。
如何让程序自动找到reader和writer的配置并自动连接,在/etc/local.json里配置。
reader的配置 from.$from_db_type.$from_app_$from_db
writer的配置 to.$to_db_type.$to_app_$to_db
params参数 
pk代表切分键,src的value对应reader表的列名
read_batch 读取批次 根据pk切分
write_batch 多少条一个批次写入
worker_num 开启多少worker并发执行任务
```

## 4.编译
```
//macOs系统
make mac 

//linux系统
make linux

```

## 启动命令
```
cd cmd

./goshift -c ../etc -e local -cmd sync -mode init -id 5


-c 配置文件的路径
-e 运行的环境 按需自己去添加dev.json/test.json/prod.json
-cmd 命令 暂支持sync 后续拓展checkTaskParams等命令
-mode init/increase 全量和增量模式 目前先支持全量
```


## 日志
```
2024-07-30T21:36:51+08:00	INFO	plugin/mysql_plugin.go:357	taskIndex:44 (start:15191022:end:15196022),wid:4,syncNum:5000,status:2

2024-07-30T21:36:51+08:00	INFO	plugin/mysql_plugin.go:341	workerid 1 executed:3

2024-07-30T21:36:51+08:00	INFO	plugin/mysql_plugin.go:332	[finished process is 48/48,unfinished is 0/48]

2024-07-30T21:36:51+08:00	INFO	plugin/mysql_plugin.go:357	taskIndex:46 (start:15201022:end:15206022),wid:1,syncNum:5000,status:2

2024-07-30T21:36:51+08:00	INFO	plugin/mysql_plugin.go:360	from mysql reader data totalSyncNum 237172 success
2024-07-30T21:36:51+08:00	INFO	scheduler/scheduler.go:203	read result is &{TaskId:5 Err:<nil> SyncNum:237172 Tm:0xc00021c900}
2024-07-30T21:36:51+08:00	INFO	scheduler/scheduler.go:134	taskid:5 result &{TaskId:5 Err:<nil> SyncNum:237172 Tm:0xc00021c900}
2024-07-30T21:36:51+08:00	INFO	scheduler/scheduler.go:139	taskid:5 success
2024-07-30T21:36:51+08:00	INFO	scheduler/scheduler.go:154	total tasks:1 successed:1,failed 0
2024-07-30T21:36:51+08:00	INFO	scheduler/scheduler.go:155	task status success
```

