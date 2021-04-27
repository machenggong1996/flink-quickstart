# flink安装

* [Mac安装Flink](https://blog.csdn.net/vbirdbest/article/details/104256807)

## flot

最大并行度有关

## 1. jar包任务运行

flink run -c com.example.helloworld.SocketTextStreamWordCount
flink-helloworld-1.0-SNAPSHOT.jar
127.0.0.1 9000

## 2. flink命令

* flink cancel
* flink list

## 3. 资源管理

yarn和k8s

### yarn

Session-cluster和Per job cluster

## 4. flink运行时架构

### flink运行时组件

JobManger

TaskManger

ResourceManger

dispatcher

### 任务提交流程

APP(提交应用)->dispatcher(启动并提交应用)->jobManager(请求slots)->
ResourceManger(启动，注册slots，发出提供slot的指令)<->taskManger(交换数据)
->JobManger(提供slot，提交要在slots中执行的任务)

### 任务调度原理

#### 1. 怎样实现并行计算

* 多线程
* 一个特定算子的子任务的个数被称之为并行度
* 一般情况下，一个stream的并行度，可以认为就是其所有算子中最大的并行度

#### 2. 并行的任务，需要占用多少个slot

和最大占用slot的任务有关

#### 3. 一个流处理程序，到底包含多少个任务

1. 有些任务会合并，什么时候会合并，什么时候不会合并

#### 4. 代码写完之后要生成多少个任务

* flink由Source，Transformation，sink三步组成

#### 5. 执行图 ExecutionGraph

* StreamGraph->JobGraph->ExecutionGraph->物理执行图

#### 6. 数据传输形式

* one-to-one
* redistributing
* 任务链：相同并行度，相同slot共享组的one-to-one操作会合并任务，这是一种优化

## 5. 流处理API

### 5.1 获取执行环境

* 获取执行环境
* 本地环境
* 远程环境

### 5.2 Source读取数据

* 从集合中读取数据
* 从文件读取数据
* 从kafka读取数据
* 自定义数据源

### 5.2 Transform转换算子

* map
* flatMap
* filter

#### 5.2.1 滚动聚合算子

* sum
* min   字段最小，但是不一定是当前记录
* max
* minBy 整行记录
* maxBy
* reduce 要先KeyBy 优点是比起minBy maxBy可以获取最新数据

#### 5.2.2 分流计算

* dataStream.split过时 使用sideOut代替

#### 5.2.3 合流计算

* ConnectedStreams MapFunction 实现map1 map2 方法

#### 5.2.4 多条流合并 union 数据类型必须相同

* DataStream.union

#### 5.2.5 UDF函数类

* Function
* Rich Functions

### 5.3 数据充分区操作

* shuffle
* reBalance
* global
* keyBy

### 5.4 sink迭代操作

* sink是写入操作，source是读取操作

## 6. 窗口API

* [Flink Window窗口机制](https://www.cnblogs.com/ronnieyuan/p/11847568.html)

* 翻滚窗口 (Tumbling Window, 无重叠)
* 滑动窗口 (Sliding Window, 有重叠)
* 会话窗口 (Session Window, 活动间隙)
* 全局窗口 (略)

## 6.1 时间窗口

* 滚动时间窗口
* 滑动时间窗口
* 会话窗口

## 6.2 计数窗口

* 滚动计数窗口
* 滑动计数窗口

## 6.3 窗口函数

* 增量聚合函数
* 全窗口函数

## b站

该看p46了