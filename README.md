# tsbs 使用手册

### 简介

agent 的压测工具，目前[支持的agent](#support-agent)

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
./benchmark load kmon --config=./config-kmon-simulator-devops.yaml
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

`use-qps-limiter` 表示是否启用固定压力模式，默认不开启，即尽发送端最大能力发送数据，尽可能测出 qps 上限

`limiter-max-qps` 当启用固定压力模式时才有意义，指定固定压力的大小

`limiter-bucket-size` <font color='red'> 当测试 kmon 时，应该保证至少为 50，当测试 otel 时，应该保证至少为 batch-size(见下) </font>，否则测试可能失败

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


### 支持的 target

#### <span id="support-agent">agent</span>

- <span id = "agent">kmon </span>
- otel
- prometheus pull mode

#### database

- timescaledb
- influxdb
- prometheus



### 压测结果

见[大盘](https://kmonitor2.alibaba-inc.com/d/cQuSzOsGk/agent-xing-neng-ce-shi?orgId=1&from=1614325694797&to=1614329084797)

