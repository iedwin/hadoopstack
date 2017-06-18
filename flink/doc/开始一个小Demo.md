# flink start
## 下载安装
- [下载http://flink.apache.org/downloads.html](http://flink.apache.org/downloads.html)
## 解压
``` shell
$ tar xzf flink-*.tgz
$ cd flink-1.3.0
```
## 启动
``` shell
$ ./bin/start-local.sh  # Start Flink
```
## 运行一个小demo
1. 运行一个socket小客户端发送数据
``` shell
$ nc -l 9009
```

2. 运行测试代码
``` shell
$ ./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9009
```

3. 查看
``` shell
$ tail -f log/flink-*-jobmanager-*.out
```

## 代码
> com.xiaoxiaomo.flink.streaming.socket.SocketWindowWordCount


- 参考：[https://ci.apache.org/projects/flink/flink-docs-release-1.3/quickstart/setup_quickstart.html](https://ci.apache.org/projects/flink/flink-docs-release-1.3/quickstart/setup_quickstart.html)
