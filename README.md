# goshift data syncer 2 other database

# 设计理念
```
mysql或者其他数据库创建一个task表。
from_db_type代表读取的数据库类型
from_app为读取的系统
from_db代表读取的库名
from_tanle为读取的表。
同理
to_db_type为写入的数据库类型
to_app为写入的系统
to_db为写入的db,
to_table为写入的表。
params字段代表参数,params的值为{"split":{"pk":["id"]},"worker_num":20,read_batch:5000,write_batch:500}

split下面的pk代表切分键,worker_num为协程数，read_batch为读取的批次大小，write_batch为写入的批次大小。

配置文件为json配置文件格式,里面内容为{"from":{"mysql":{$from_app_$from_db:{数据库链接}}}。

当运行一个任务时，先读取数据库得到一条数据。根据from_db_type得到客户端，比如from_db_type=mysql，说明读取者是mysqlclient,from_app+from_db 组成唯一的key，从json文件里可以获取到reader链接数据库的参数。同理to_db_type=mysql，也可以获取mysqlclient的链接信息。
为了考虑拓展性，这个项目下有plugin目录,plugin_interface.go里面定义reader的接口，reader接口有Connect方法可以自动读取config配置创建数据库链接，同时再有Read方法，根据params参数自动切分并行执行读取数据。再定义writer接口，writer接口有Connect方法可以自动读取config配置创建数据库链接,同时再有Write方法,根据write_batch切分写入。
使用Go开发充分利用并发执行任务能力。


```


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
