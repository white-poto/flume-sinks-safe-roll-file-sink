
flume-safe-roll-file-sink 
=========================
This plugin has been used in production environment more than a year.  
[中文README.MD](https://github.com/huyanping/flume-sinks-safe-roll-file-sink/blob/master/README.ZH.MD)
Feature：
-------------------------------
+ add suffix to the file after it is finished.
+ move file to another place after it is finised.
+ delete empty file automatically
+ copy file to other paths

Download Jar
------------
[safe-roll-file-sink-0.2.jar](https://github.com/huyanping/flume-sinks-safe-roll-file-sink/releases)


Configuration：
--------------
- sink.moveFile	default:false	if move the file
- sink.targetDirectory	default:''	the path which the files will be moved to
- sink.useFileSuffix	default:false	if add suffix when moving or copying the file
- sink.fileSuffix	default:.COMPLEATED	file suffix
- sink.useCopy		default:false		if copy the file
- sink.copyDirectory	default:''		the paths which the files will be copied to

Configuration Example：
----------------------
```shell

client.sources = source_client
client.sinks = sink_client
client.channels = channel_client

# source configurations
client.sources.source_client.type = spooldir
client.sources.source_client.channels = channel_client
client.sources.source_client.spoolDir = /some_logs_dir/
client.sources.source_client.fileHeader = true

# sink configurations
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

# channel configurations
client.channels.channel_client.type = file
client.channels.channel_client.checkpointDir = /data/tmp/checkpoint
client.channels.channel_client.dataDirs = /data/tmp
```


