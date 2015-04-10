flume-safe-roll-file-sink 
===========================
flume roll file sink升级版，支持以下特性：
-------------------------------
+ 支持针对已处理完成文件添加后缀
+ 支持已处理完成文件移动
+ 支持空文件删除（当进行文件移动或文件重命名时，如文件为空，则执行delete()操作）
+ 支持文件多路复制

配置参数：
----------------------
- sink.moveFile	default:false	含义：是否移动文件
- sink.targetDirectory	default:''	含义：移动目标目录
- sink.useFileSuffix	default:false	含义：是否添加文件后缀（移动和复制过程均会使用后缀名）
- sink.fileSuffix	default:.COMPLEATED	含义：文件后缀名
- sink.useCopy		default:false		含义：是否复制文件
- sink.copyDirectory	default:''		含义：复制目标目录（多个目录请使用英文逗号分隔）

配置示例：
--------------------
```shell
# 负载机flume client配置

# 组件名称配置
client.sources = source_client
client.sinks = sink_client
client.channels = channel_client

# 日志源组件配置（监听目录下新增文件）
client.sources.source_client.type = spooldir
client.sources.source_client.channels = channel_client
client.sources.source_client.spoolDir = /some_logs_dir/
client.sources.source_client.fileHeader = true

# sink组件配置
client.sinks.sink_client.type = cn.huyanping.flume.sinks.SafeRollingFileSink
client.sinks.sink_client.channel = channel_client
client.sinks.sink_client.sink.directory = /data/source
client.sinks.sink_client.sink.rollInterval = 1

client.sinks.sink_client.sink.moveFile = true
client.sinks.sink_client.sink.targetDirectory = /data/target

client.sinks.sink_client.sink.useFileSuffix = true
client.sinks.sink_client.sink.fileSuffix = .COMPLETED

client.sinks.sink_client.sink.useCopy = true
client.sinks.sink_client.sink.copyDirectory = /data/copy1,/data/copy2

# 文件管道设置
client.channels.channel_client.type = file
client.channels.channel_client.checkpointDir = /data/tmp/checkpoint
client.channels.channel_client.dataDirs = /data/tmp
```

