主要功能

基于flume的AbstractSink，自定义扩展支持mysql（也适用于doris）的sink，使用了druid作为数据库连接池；同时基于flume的SinkCounter实现了mysqlsink的监控支持

flume配置样例

```
# 定义这个agent中各组件的名字
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# 描述和配置source组件：r1
a1.sources.r1.type=TAILDIR
a1.sources.r1.filegroups=f1
a1.sources.r1.filegroups.f1=/home/nginx/log/.*

# 描述和配置sink组件：k1
a1.sinks.k1.type =com.mzdd.flume.business.sink.DemoDataSink
a1.sinks.k1.hostname=127.0.0.1
a1.sinks.k1.port=3306
a1.sinks.k1.databaseName=demo
a1.sinks.k1.tableName=demo_data
a1.sinks.k1.user=root
a1.sinks.k1.password=123456
a1.sinks.k1.batchSize=100


# 描述和配置channel组件，此处使用是内存缓存的方式
a1.channels.c1.type = memory
a1.channels.c1.capacity = 200
a1.channels.c1.transactionCapacity = 100

# 描述和配置source  channel   sink之间的连接关系
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

启动命令

```shell
/home/apache-flume-1.11.0-bin/bin/flume-ng agent --conf conf --conf-file /home/conf/flume/demo.conf -n a1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 -Dflume.root.logger=debug,console 
```

