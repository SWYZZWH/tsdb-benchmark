**目前支持[裸工具版本](#tsbs)与[http服务版本](#tsbs-server)**

[TOC]


## 支持的 target

### <span id="support-agent">agent</span>

- <span id = "agent">kmon </span>
- otel
- prometheus pull mode

### database

- timescaledb
- influxdb
- prometheus


## <span id="tsbs">tsbs 使用手册</span>

### 简介

时序数据库压测工具

原项目文档见 **https://github.com/timescale/tsbs**



### 构建

#### 获取代码

```bash
git clone git@gitlab.alibaba-inc.com:monitor_service/tsdb_benchmark.git
git checkout kmon-support
cd tsdb_benchmark
```

目前支持压测 agent 的分支为 kmon-support

#### 编译

```bash
go build -o benchmark ./cmd/tsbs_load
```

交叉编译linux版本

```bash
env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o benchmark_linux ./cmd/tsbs_load
```



### 运行

根目录下自带了配置文件 `config-kmon-simulator-devops.yaml` 和 `config-otel-simulator-devops.yaml`，可直接修改使用

```bash
./benchmark load kmon --config=./config-kmon-simulator.yaml
```

```bash
./benchmark load otel --config=./config-otel-simulator-devops.yaml
```

也支持生成配置文件

```bash
./benchmark config --target=otel --data-source=SIMULATOR
```

将在根目录生成 `config.yaml `文件



### 功能与配置文件说明

重要的配置项有：

##### 线数目 scale

```yaml
data-source:
  simulator:
  	scale: "1"
  	timestamp-end: "2020-01-02T00:00:00Z"
    timestamp-start: "2020-01-01T00:00:00Z"
```

Scale 表示有多少条线 / 有多少台不同的主机在汇报指标

##### 数据点总数 timespan

```yaml
data-source:
  simulator:
  	timestamp-end: "2020-01-02T00:00:00Z"
    timestamp-start: "2020-01-01T00:00:00Z"
```

压测持续时间较短时，设置一个很大的 `timestamp-end` 可以延长压测持续的时间

##### 并发数/连接数 workers

```yaml
loader:
  runner:
  	workers: "100"
```

与 agent 建立多少个链接，如果为了获得压力发送端的最大能力，直接设置成 100 即可

##### 固定发送压力(qps)

```yaml
loader:
  db-specific:
  	use-qps-limiter: false
    limiter-max-qps: 1000
    limiter-bucket-size: 1200
```

`use-qps-limiter` 表示是否启用固定压力模式，默认不开启，即尽发送端最大能力发送数据，尽可能测出 qps 上限

`limiter-max-qps` 当启用固定压力模式时才有意义，指定固定压力的大小

`limiter-bucket-size` <font color='red'> 当测试 kmon 时，应该保证至少为 50，当测试 otel 时，应该保证至少为 batch-size(见下) </font>，否则测试可能失败

**注意**：一般情况下，实际压力与设定值的差异不超过5%，但当 limiter-bucket-size 较大，或设定的压力接近发送端的能力上限时，可能差异会较大。另外，由于分批发送，当设定压力小而批较大时，差异也会比较大

##### 批大小 batch-size

- kmon

  ```yaml
  loader:
    db-specific:
      send-batch-size: 1000
    runner:
      batch-size: "100"
  ```

  注意，这里 **send-batch-size** 表示压测工具每次发给 agent 的数据批大小

  而 **batch-size** 则与数据生成有关，一般不需要关心

- otel

  在 otel 中，**runner中的batch-size就是发送的batch-size**

  ```yaml
  loader:
    runner:
    	batch-size: "1000"
  ```



### 获取压测结果

otel/kmon agent

- 见大盘

prometheus pull mode

- http://localhost:port/metrics port在配置文件中配置




### 压测结果

见[大盘](https://kmonitor2.alibaba-inc.com/d/cQuSzOsGk/agent-xing-neng-ce-shi?orgId=1&from=1614325694797&to=1614329084797)





## <span id="tsbs-server">tsbs-server 使用手册</span>

### 简介

tsbs 压测工具的http服务版，支持使用 RESTful apis 启停压测，配置压测参数，查看报告



### 手动构建

#### 获取源码

```bash
git clone git@gitlab.alibaba-inc.com:monitor_service/tsdb_benchmark.git
git checkout http-service
cd tsdb_benchmark
```

#### 提前配置好 go env

```bash
export GOPROXY=https://goproxy.io && export GOPRIVATE=gitlab.alibaba-inc.com
 && export GONOPROXY=gitlab.alibaba-inc.com && export GONOSUMDB=gitlab.alibaba-inc.com
```

#### 编译 tsbs 与 server

```bash
go build -o ./bin/benchmark ./cmd/tsbs_load
go build -o ./bin/server ./server
```

#### 运行

```bash
./bin/server -p=8888
```

可指定端口，默认监听 8888 端口



### docker

```bash
docker build -t tsbs-server-0.1 .
```

运行时注意暴露端口



### API

#### 启动

```bash
curl "http://localhost:8888/start?db=prom-pull"
```

启动默认参数配置的压测，压测 target 为 prom-pull，默认配置文件此时为 `./config/config-prom-pull-simulator.yaml`

为保证不会相互干扰，同时只能运行一个压测程序，如果有一个压测程序正在运行，会返回：

> "another benchmark is running, use /stop api to shutdown first"

某些参数配置错误时，不会报错，但压测也不会启动，可以连续启动几次压测，如果不报上述错误，说明参数配置错误，或压测时间过短



#### 停止

```bash
curl "http://localhost:8888/stop"
```



#### 压测参数

以下几个参数适用于所有 target

- db (压测target)【必填】不填将报错
- workers (制造压力的线程数)【选填】
- scale(数据线数目) 【选填】
- timestamp-end(控制压测的持续时间/影响总的点数)【选填】**不表示压测结束的时间！**一般情况下可以设置尽可能的大，如9999-01-02T00:00:00Z
- usecase(压测场景/测试用例) 【选填】一般不要填，使用默认的devops场景就可以

不同 target 可以有特定参数，以otel为例

- host (otel collector的ip)【选填】
- port (otel collector的端口)【选填】
- use-qps-limiter (是否使用固定压力模式)【选填】true/false
- limiter-max-qps (固定压力大小)【选填】float64
- limiter-bucket-size (固定压力下的桶大小) 一般不需要设置

**示例**

```bash
curl "http://localhost:8888/start?db=prom-pull&workers=2&scale=8&timestamp-end=9999-01-02T00:00:00Z&port=8111"
```



#### 查看报告

接口开发中，目前可以直接查看 `./log.txt`

