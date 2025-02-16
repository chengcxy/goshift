# goshift data syncer 2 other database

# 一.设计理念
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


运行模式，命令行参数可以执行单个主键id 
当命令行解析参数后查询task_id再数据库里的配置，params参数设置了worker_num=20 则执行数据同步任务时会继续起20个协程。
为了减少对数据库的读写压力，减少并发资源占用,建议不超过20个线程

Run方法逻辑:
  1.1 根据命令行参数获取task_id,加载数据库数据,赋值job.TaskMeta指针
  1.2 根据from_db_type/to_db_type获取plugin的reader和writer.
  1.3 解析数据库的数据的taskMeta,确定起多少个worker.
  1.4 调用reader.SplitTaskParams方法获取一共需要执行多少任务
  1.5 创建tasks channel,result channel
  1.6 调度器的worker函数监听tasks channel，获取待执行的分片任务参数,reader负责根据分片任务参数读取数据，调用writer的write方法写到目标库,更新完成进度
  1.7 监听结果通道,打印同步进度


后续拓展：
api接口管理能力,前端页面的异步执行查看进度功能,因函数封装了执行单个task_id的逻辑,后续可封装成Event,生成异步任务id,扔到消息队列,
启动一个消费者监听消费队列更新任务状态，形成闭环



```


## 二.创建数据库表
```
在配置文件local.json里task_meta.db参数所在的库建表

CREATE TABLE `task_def_sync_manager` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '同步任务id',
  `from_app` varchar(50) DEFAULT NULL COMMENT '读取的业务系统',
  `from_db_type` varchar(50) DEFAULT NULL COMMENT '读取的数据源类型',
  `from_db` varchar(20) DEFAULT NULL COMMENT '读取的数据库',
  `from_table` varchar(255) DEFAULT '' COMMENT '读取表',
  `to_app` varchar(50) DEFAULT NULL COMMENT '写入的业务系统',
  `to_db_type` varchar(20) DEFAULT NULL COMMENT '写入数据源类型',
  `to_db` varchar(20) DEFAULT NULL COMMENT '写入数据库',
  `to_table` varchar(255) DEFAULT NULL COMMENT '写入数据表',
  `params` text COMMENT '规则',
  `online_status` int(11) DEFAULT NULL COMMENT '在线状态1在线0下线',
  `task_desc` text COMMENT '任务描述',
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


## 三.配置文件里的环境变量赋值

```
查看etc目录下某个环境(以local环境linux或mac系统为案例)task_meta的配置，设置环境变量,变量名自定义和配置文件保持一致即可。
export LOCAL_DW_MYSQL_Z_ETL_HOST="localhost"
export LOCAL_DW_MYSQL_Z_ETL_USER="z_etl"
export LOCAL_DW_MYSQL_Z_ETL_PASSWORD="密码自定义"
```

## 四.编译 build

```
//linux系统
make linux

//macOs系统
make mac
```

## 五.命令行参数


```
➜  cmd git:(master) ✗ ./goshift --help

Usage of ./goshift:
  -UsedEnv
    	是否走环境变量 (default true)
  -c string
    	配置文件目录 (default "../etc/")
  -cmd string
    	命令 (default "sync")
  -e string
    	运行的环境-json文件前缀 dev/test/prod/local (default "local")
  -id string
    	任务id (default "1")
  -mode string
    	模式 (default "init")
```


## 六.demo

```
./goshift -c ../etc/ -e local -id 5
```