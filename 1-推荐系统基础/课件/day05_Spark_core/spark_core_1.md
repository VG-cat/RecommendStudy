## spark 入门

课程目标：

- 了解spark概念
- 知道spark的特点（与hadoop对比）
- 独立实现spark local模式的启动

### 1.1 spark概述

- 1、什么是spark

  - 基于内存的计算引擎，它的计算速度非常快。但是仅仅只涉及到数据的计算，并没有涉及到数据的存储。

- 2、为什么要学习spark

  **MapReduce框架局限性**

  - 1，Map结果写磁盘，Reduce写HDFS，多个MR之间通过HDFS交换数据
  - 2，任务调度和启动开销大
  - 3，无法充分利用内存
  - 4，不适合迭代计算（如机器学习、图计算等等），交互式处理（数据挖掘）
  - 5，不适合流式处理（点击日志分析）
  - 6，MapReduce编程不够灵活，仅支持Map和Reduce两种操作

  **Hadoop生态圈**

  - 批处理：MapReduce、Hive、Pig
  - 流式计算：Storm
  - 交互式计算：Impala、presto

  需要一种灵活的框架可同时进行批处理、流式计算、交互式计算

  - 内存计算引擎，提供cache机制来支持需要反复迭代计算或者多次数据共享，减少数据读取的IO开销
  - DAG引擎，较少多次计算之间中间结果写到HDFS的开销
  - 使用多线程模型来减少task启动开销，shuffle过程中避免不必要的sort操作以及减少磁盘IO

  spark的缺点是：吃内存，不太稳定

- 3、spark特点

  - 1、速度快（比mapreduce在内存中快100倍，在磁盘中快10倍）
    - spark中的job中间结果可以不落地，可以存放在内存中。
    - mapreduce中map和reduce任务都是以进程的方式运行着，而spark中的job是以线程方式运行在进程中。
  - 2、易用性（可以通过java/scala/python/R开发spark应用程序）
  - 3、通用性（可以使用spark sql/spark streaming/mlib/Graphx）
  - 4、兼容性（spark程序可以运行在standalone/yarn/mesos）



Apache Spark 是一个快速、通用的分布式计算系统，支持大规模数据处理。以下是 Spark 的安装步骤：

---

### 安装

#### **1. 前置条件**
在安装 Spark 之前，确保满足以下条件：
1. **Java**：Spark 需要 Java 8 或更高版本。
   - 检查 Java 版本：
     ```bash
     java -version
     ```
   - 如果未安装 Java，可以使用以下命令安装：
     ```bash
     sudo apt update
     sudo apt install openjdk-8-jdk
     ```

2. **Hadoop（可选）**：
   - 如果需要在 Hadoop 集群上运行 Spark，需要先安装 Hadoop。
   - 如果只是本地运行 Spark，可以跳过 Hadoop 安装。

3. **Scala（可选）**：
   - Spark 是用 Scala 编写的，但可以使用 Java、Python 或 R 编写 Spark 程序。
   - 如果需要使用 Scala，可以安装 Scala：
     ```bash
     sudo apt install scala
     ```

---

#### **2. 下载 Spark**
1. 访问 Spark 的官方下载页面：
   [Apache Spark Downloads](https://spark.apache.org/downloads.html)
2. 选择适合的版本：
   - **Spark 版本**：推荐最新稳定版（如 3.x）。
   - **Package Type**：选择与 Hadoop 兼容的预编译包（如 `Pre-built for Apache Hadoop 3.x`）。
3. 下载 Spark：
   ```bash
   wget https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
   ```

---

#### **3. 解压 Spark**
将下载的 Spark 压缩包解压到目标目录：
```bash
tar -zxvf spark-3.3.1-bin-hadoop3.tgz -C /opt/
```

---

#### **4. 配置环境变量**
编辑 `~/.bashrc` 或 `/etc/profile` 文件，添加 Spark 的环境变量：
```bash
export SPARK_HOME=/opt/spark-3.3.1-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

使配置生效：
```bash
source ~/.bashrc
```

---

#### **5. 配置 Spark**
1. **复制配置文件模板**：
   Spark 提供了配置文件的模板，复制并重命名：
   ```bash
   cd $SPARK_HOME/conf
   cp spark-env.sh.template spark-env.sh
   cp spark-defaults.conf.template spark-defaults.conf
   cp log4j2.properties.template log4j2.properties
   ```

2. **配置 `spark-env.sh`**：
   编辑 `spark-env.sh`，设置以下参数：
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # Java 安装路径
   export SPARK_MASTER_HOST=localhost                  # Spark Master 主机名
   export SPARK_WORKER_MEMORY=4g                      # 每个 Worker 的内存
   ```

3. **配置 `spark-defaults.conf`**：
   编辑 `spark-defaults.conf`，设置默认参数：
   ```bash
   spark.master                     spark://localhost:7077
   spark.eventLog.enabled           true
   spark.eventLog.dir               file:///tmp/spark-events
   spark.serializer                 org.apache.spark.serializer.KryoSerializer
   ```

4. **配置日志级别**：
   编辑 `log4j2.properties`，调整日志级别（如将 `INFO` 改为 `WARN`）：
   ```bash
   rootLogger.level = WARN
   ```

---

#### **6. 启动 Spark**
1. **启动 Master 节点**：
   ```bash
   $SPARK_HOME/sbin/start-master.sh
   ```
   - 访问 `http://localhost:8080`，查看 Spark Master Web UI。

2. **启动 Worker 节点**：
   ```bash
   $SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
   ```
   - 访问 `http://localhost:8081`，查看 Spark Worker Web UI。

3. **启动 Spark Shell**：
   - 启动 PySpark Shell：
     ```bash
     pyspark
     ```
   - 启动 Scala Shell：
     ```bash
     spark-shell
     ```

---

#### **7. 测试 Spark**
##### **运行示例程序**
1. 使用 PySpark 运行 WordCount 示例：
   ```python
   text_file = sc.textFile("README.md")
   counts = text_file.flatMap(lambda line: line.split(" ")) \
                     .map(lambda word: (word, 1)) \
                     .reduceByKey(lambda a, b: a + b)
   counts.saveAsTextFile("output")
   ```

2. 使用 Scala 运行 WordCount 示例：
   ```scala
   val textFile = sc.textFile("README.md")
   val counts = textFile.flatMap(line => line.split(" "))
                        .map(word => (word, 1))
                        .reduceByKey(_ + _)
   counts.saveAsTextFile("output")
   ```

3. 检查输出结果：
   ```bash
   hdfs dfs -cat output/part-00000
   ```

---

#### **8. 停止 Spark**
1. 停止 Worker 节点：
   ```bash
   $SPARK_HOME/sbin/stop-worker.sh
   ```

2. 停止 Master 节点：
   ```bash
   $SPARK_HOME/sbin/stop-master.sh
   ```

---

#### **9. 常见问题**
1. **Java 版本不兼容**：
   确保 Java 版本与 Spark 兼容（推荐 Java 8 或 11）。
2. **端口冲突**：
   如果端口 8080 或 7077 被占用，修改 `spark-env.sh` 中的端口配置。
3. **内存不足**：
   调整 `spark-env.sh` 中的 `SPARK_WORKER_MEMORY` 参数。

---

通过以上步骤，你可以成功安装并配置 Spark，开始进行大规模数据处理。

### 1.2 spark启动（local模式）和WordCount(演示)

- 启动pyspark

  - 在$SPARK_HOME/sbin目录下执行

    - ./pyspark

  - ![](/img/pyspark.png)

  - ``` python
    sc = spark.sparkContext
    words = sc.textFile('file:///home/hadoop/tmp/word.txt') \
                .flatMap(lambda line: line.split(" ")) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b).collect()
    ```

  - 输出结果：

    ```shell
    [('python', 2), ('hadoop', 1), ('bc', 1), ('foo', 4), ('test', 2), ('bar', 2), ('quux', 2), ('abc', 2), ('ab', 1), ('you', 1), ('ac', 1), ('bec', 1), ('by', 1), ('see', 1), ('labs', 2), ('me', 1), ('welcome', 1)]
    
    ```




### 保存回 HDFS
在 Spark 中，计算结果可以通过多种方式保存回 HDFS。以下是一些常见的方法：

---

#### **1. 使用 `saveAsTextFile` 保存为文本文件**
将计算结果保存为文本文件到 HDFS。

##### **示例代码**
```python
# 假设 rdd 是计算结果
rdd = sc.parallelize([("apple", 3), ("banana", 2), ("orange", 5)])

# 保存到 HDFS
rdd.saveAsTextFile("hdfs://namenode:8020/output/path")
```

##### **说明**
- 输出目录中的文件是分区的（如 `part-00000`、`part-00001` 等）。
- 如果输出目录已存在，会报错。可以手动删除目录或设置覆盖选项。

---

#### **2. 使用 `saveAsSequenceFile` 保存为 SequenceFile**
将计算结果保存为 SequenceFile 格式（适用于键值对数据）。

##### **示例代码**
```python
# 假设 rdd 是键值对数据
rdd = sc.parallelize([("apple", 3), ("banana", 2), ("orange", 5)])

# 保存到 HDFS
rdd.saveAsSequenceFile("hdfs://namenode:8020/output/path")
```

##### **说明**
- SequenceFile 是 Hadoop 的一种二进制文件格式，适合存储键值对数据。

---

#### **3. 使用 `saveAsHadoopFile` 保存为其他 Hadoop 文件格式**
支持保存为其他 Hadoop 文件格式（如 Avro、Parquet 等）。

##### **示例代码**
```python
# 假设 rdd 是键值对数据
rdd = sc.parallelize([("apple", 3), ("banana", 2), ("orange", 5)])

# 保存为 Parquet 文件
rdd.saveAsHadoopFile(
    "hdfs://namenode:8020/output/path",
    "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
)
```

---

#### **4. 使用 DataFrame 的 `write` 方法**
如果使用 Spark SQL 或 DataFrame API，可以使用 `write` 方法将结果保存为多种格式（如 Parquet、JSON、CSV 等）。

##### **示例代码**
```python
# 假设 df 是 DataFrame
df = spark.createDataFrame([("apple", 3), ("banana", 2), ("orange", 5)], ["fruit", "count"])

# 保存为 Parquet 文件
df.write.parquet("hdfs://namenode:8020/output/path")

# 保存为 CSV 文件
df.write.csv("hdfs://namenode:8020/output/path")

# 保存为 JSON 文件
df.write.json("hdfs://namenode:8020/output/path")
```

##### **说明**
- `write` 方法支持多种文件格式和选项（如分区、压缩等）。
- 可以使用 `mode` 参数指定保存模式：
  - `append`：追加数据。
  - `overwrite`：覆盖数据。
  - `ignore`：如果路径已存在，忽略操作。
  - `error`（默认）：如果路径已存在，报错。

---

#### **5. 使用 `saveAsNewAPIHadoopFile` 保存为新的 Hadoop API 格式**
适用于使用新的 Hadoop API 保存数据。

##### **示例代码**
```python
# 假设 rdd 是键值对数据
rdd = sc.parallelize([("apple", 3), ("banana", 2), ("orange", 5)])

# 保存到 HDFS
rdd.saveAsNewAPIHadoopFile(
    "hdfs://namenode:8020/output/path",
    "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
)
```

---

#### **6. 使用 `coalesce` 或 `repartition` 控制输出文件数量**
默认情况下，Spark 会根据分区数生成多个输出文件。可以使用 `coalesce` 或 `repartition` 控制输出文件的数量。

##### **示例代码**
```python
# 减少输出文件数量
rdd.coalesce(1).saveAsTextFile("hdfs://namenode:8020/output/path")

# 增加输出文件数量
rdd.repartition(10).saveAsTextFile("hdfs://namenode:8020/output/path")
```

---

#### **7. 覆盖已存在的输出目录**
如果输出目录已存在，默认会报错。可以通过以下方式覆盖目录：

##### **方法 1：手动删除目录**
```bash
hdfs dfs -rm -r /output/path
```

##### **方法 2：使用 `overwrite` 模式（DataFrame API）**
```python
df.write.mode("overwrite").parquet("hdfs://namenode:8020/output/path")
```

##### **方法 3：使用 `saveAsTextFile` 时先检查目录**
```python
from py4j.protocol import Py4JJavaError

try:
    rdd.saveAsTextFile("hdfs://namenode:8020/output/path")
except Py4JJavaError as e:
    if "FileAlreadyExistsException" in str(e):
        hdfs.delete("/output/path", recursive=True)
        rdd.saveAsTextFile("hdfs://namenode:8020/output/path")
```

---

#### **8. 检查保存结果**
保存完成后，可以使用 HDFS 命令检查输出文件：
```bash
hdfs dfs -ls /output/path
hdfs dfs -cat /output/path/part-00000
```

---

#### **总结**
- 使用 `saveAsTextFile`、`saveAsSequenceFile` 等方法保存 RDD 数据。
- 使用 DataFrame 的 `write` 方法保存结构化数据。
- 控制输出文件数量和格式，确保数据按需存储。

通过以上方法，你可以轻松将 Spark 的计算结果保存回 HDFS。



