# Accompany-Vehicle-Discovery
### 实验背景

伴随车，指的是在规定的时间内某些车辆一起通过的卡口数量达到了某一阈值，这些车辆即为伴随车辆。挖掘大量已识别的车牌数据中的内在联系，检测伴随车辆，已然成为了交通管理者关注的热点问题。这是因为伴随车辆往往具有团队作案的嫌疑，所以伴随车辆的发

现对治安有十分重要的意义。如果事先知道涉案车辆的车牌号码，可以通过查看其轨迹数据找出与其存在伴随关系的车辆。但是在实际情况中通常不知道涉案车辆的车牌，所以就需要通过一定方法和技术从海量车牌识别数据中挖掘出存在伴随关系的车辆。我们在此提出了两种解决方案，一种是基于 FP-Growth 算法的 Spark 分布式解决方案，一种则是基于 SparkSQL的解决方案。最终，我们在耗时较短的情况下得到了尽可能多的伴随车辆。

### Spark

#### 2.1 Spark 数据预处理

采用窗口伴随的方法对伴随车进行定义，引入滑动时间窗口的概念。滑动时间窗口是一个数据结构，它具有固定的时间跨度 ，并且以该时间跨度滑过需要检验的数据。

首先，将点 i 在给定时间跨度内的过车数据按时间排序成一个队列。然后用滑动时间窗口在该队列上滑动，并将时间窗口的时间跨度设置为 tmax。在时间窗口滑动的过程中，当时间窗口覆盖的过车数大于 1 时，将窗口内的车辆作为结果返回，则被返回的车辆，满足伴随车辆查询的定义要求。

根据上述的方法对数据进行了预处理，把文件按时间分成 5 份，每份文件中的数据都是若干列表，一个列表中包含了一个路口 60s 时间段内的所有车。

主要的操作流程见代码：经过结构化数据处理以后，读取数据作为算法的输入，通过处理得到频繁项集。首先去除格式化的符号和一个窗口伴随内的重复车辆，然后再去除空集。

#### 2.2 Spark 算法

2.2.1 以往算法

以往伴随车相关算法主要有两种，一种是基于轨迹相似度的算法，另一种是基于关联规则的挖掘算法。基于轨迹相似度的算法是通过构建车辆之间的卡口过车序列，求得车辆之间行驶轨迹的最长公共子序列，当最长公共子序列的长度达到设定阈值，则判定这些车辆为伴随车辆。基于关联规则的挖掘算法是将一定时间范围内经过卡口的车辆集合作为事务集，通过设定的支持度和置信度阈值，挖掘出相关联的车辆即伴随车辆。关联规则的挖掘算法中还有两类，分别是基于采用时间滑动窗口和基于车牌识别流式数据。我们小组主要采用是基于

采用时间滑动窗口的关联规则挖掘算法。关联规则挖掘算法基本步骤为：步骤一为根据事务集发现满足最小支持度阈值的所有项集，即频繁项集；步骤二位从频繁项集中提取具有高置信度同时大于最小支持度的关联规则。

2.2.2 关联规则挖掘算法基本概念

在进行具体算法讨论之前，我们需要先确定以下几个我们在算法中会使用到的基本概念支持度：表示事务集中包含某项记录所占的比例，用 Sup 表示。如果用 P(A) 表示包含 A 事务的 比例，那么 Sup = P(A) 。

**置信度**：针对关联规则定义，指同时包含 A 的事务中同时包含 B 事务的比例，即同时包含A 和 B 的事务占包含 A 事务的比例，用 Conf 表示。公式表达为：Conf = P(A&B)/P(A)。

**伴随车辆**：伴随车辆指在某一特定时间段内，多个车辆一起通过多个卡点，且时间差小于预设时间阈值，当检测次数超过假定检测阈值时，称这些通过多个卡点的车辆为伴随车辆。

**强关联伴随车辆**：伴随车辆不满足随机伴随情况，即车辆中不存在某一车辆检测的次数远高于其他车辆的情况，称这组伴随车辆为强关联伴随车辆。

2.2.3 伴随车生成算法方案总体设计

（1）将数据存储在 Hadoop 大数据平台中的 Hive 数据库中；

（2）基于 Spark 计算框架利用滑动时间窗口数据结构得到的伴随车辆数据集；

（3）基于 Spark 并行分布式框架的关联规则挖掘算法得到伴随车辆发现，得到伴随车辆结果。

2.2.4 Apriori 算法

（1）基本思想

该算法的基本思想是首先扫描数据集，得到每个项的支持度，经过剪枝，筛选出大于最小支持度的项，也就是 1-频繁项集。然后，算法重新扫描数据集得到上一次迭代的 k-频繁项集的支持度，剪枝处理，筛选出大于最小支持度的项集，也就是（k+1）项集，依次迭代，直到频繁项集为空时，循环结束。最后由频繁项集产生关联规则，算法结束。

（2）算法步骤：

i 计算频繁 1-项集和 2-项集

a. 读取 HDFS 中的原始事务数据库

b. 调用 flatmap()函数，可以得到一个存储项目 RDD，这个 RDD 逻辑上包含事务数据库的全部数据

c. 通过调用 reduceByKey()函数可以计算每个项目在原始事务数据库中的支持数，得到一个存储的 RDD，其中 value 值为项目在事务数据库中实际的支持数，得到了键值对的 RDD。

d.对上一步 RDD 执行 filter 函数，该函数通过 value 值与最小支持数对比找出符合要求的键值对，获得了一个的 RDD

e. 提取上一步 RDD 中的 key 值为频繁 1-项集。

f. 根据频繁 1-项集与自己连接得到候选 2-项集，再重复 d 步可以得到频繁 2-项集ii 由频繁 k-项集集合迭代计算出频繁(k+1)-项集（k≥1），直到算法停止

a.首先判断频繁 k-项集集合中的个数是否大于等于 k+1，此为计算频繁(k+1) - 项集的前提条件。若判断成立，则算法继续执行，否则算法过程中止，算法结束

b.对于频繁 k-项集的 RDD 调用 map()函数，然后对频繁 k-项集的任意两个元素，求其交集，若其长度等于 k+1，则加入到候选 k+1 项集列表。最终计算得到候选(k+1)-项集

c. 将此时得到的候选(k+1)-项集转换成为 RDD。通过调用 mapToPair()和 reduceByKey()函数可以计算每个项目的支持数，通过 filter 函数将 value 值与最小支持数对比找出符合要求的键值对，并按照 value 值升序排列获得了一个 RDD，从而得到频繁(k+1)-项集。

（3）算法缺陷

Apriori 算法使用了逐层搜索的迭代方法，开创了使用支持度的剪枝方法。但是在算法处理过程中为了得到项集的支持度会需要多次扫描数据集，增加 I/O 次数，同时也产生大量无用的候选项集，挖掘效率较低。

2.2.5 Fp-Growth 算法

（1）算法基本思想

Fp-Growth 算法通过构建 FP-tree 产生频繁项集，相较于 Apriori 算法，无需产生候选集，而且只需要 2 次扫描数据集，提高了挖掘的效率。

FP-Growth 算法的基本思想是不断地迭代 FP-Tree 的构造和投影过程，对 FP-Tree 进行递归挖掘找出所有的频繁项集。该算法两次扫描事务集：先扫描一遍数据集，得到频繁 1 项集，根据预先设定的最小支持度，删除哪些不满足的项，并按照支持度降序排序得到头表，然后将原始数据集中的条目按头表次序进行排序；第二次扫描，基于数据集构建 FP-Tree，对于每个项找到其条件模式基，递归调用树结构，删除小于最小支持度的项。如果最终呈现单一路径的树结构，则直接列举所有组合；非单一路径的则继续调用树结构，直到形成单一路径即可，再通过枚举所有可能组合并与此树的前缀连接即可得到频繁模式；最后从发现的频繁项集中提取出满足最小置信度的规则，这些规则称作强关联规则。

这里我们通过调用 org.apache.spark.mllib.fpm 中的 FPGrowth 来从窗口伴随集合中挖掘伴随车集合。

由于这里刚开始的时候把最小置信度和最小支持度设置的比较小，而且 Fp-growth 算法要递归生成 FP-tree 所以内存开销还是比较大，在用小数据跑的时候可以得到结果，但是如果用整个结构化处理后的数据进行实验则出现了 OutOfMemoryError 等错误。

2.2.6 Spark 内存使用分析

Spark 的内存管理模块在整个系统中十分重要，其主要目的为充分利用内存。在采用Spark SQL 执行 FPGrowth 算法时，发现了内存不足的问题，因此考虑采用优化 Spark 内存的方式解决这一问题。

FPGrowth 需要递归生成 FPtree，因此内存开销大首先对 Spark 的内存使用进行分析，并针对内存使用的特点逐一进行优化。在执行 Spark的应用程序时，Spark 集群会启动 Driver 和 Executor 两种 JVM 进程。

Driver 进程为主控进程，负责创建 Spark 上下文、提交 Spark 作业并将作业(Job)转化为计算任务(Task)以及在各个 Executor 进程间协调任务的调度。Executor 进程负责在工作节点上执行具体的计算任务，并将结果返回给 Driver。同时，Executor 须为持久化的 RDD提供存储功能。下针对 Executor 的内存管理进行分析。

Executor 作为 JVM 进程，其内存管理建立在 JVM 的内存管理之上，且 Spark 在此基础上对堆内(On-heap)空间进行了更详细的分配，并引入堆外(Off-heap)内存使之可以直接在工作节点的系统内存中开辟空间。Spark 为存储内存和执行内存的管理提供了统一接口——MemoryManager，同一个 Executor 内的任务均调用这一接口来申请或释放内存。Spark1.6前采用静态管理（Static Memory Manager）方式，1.6 后采用统一管理(Unified Memory Manager)方式，本实验配置为 Spark2.4.4，故针对统一管理方式进行分析。

（1）堆内外内存管理

Executor 内运行的并发任务共享 JVM 堆内内存。这些任务在缓存 RDD 数据和广播（Broadcast）数据时占用存储（Storage）内存；在执行 Shuffle 时占用执行（Execution）内存；剩余部分不作特殊规划，如 Spark 内部的对象实例或用户定义的 Spark 应用程序中的对象实例均占用剩余空间。

根据 SPARK-12081:Make unified memory management work with small heaps，若给定内存较低，根据百分比分配 reserved memory 易导致内存溢出(Out of Memory)异常，因此 Spark2.x 设置预留内存(reserved memory)固定值为 300M，其值不能修改，故在进行内存优化时不作考虑。

堆内内存采用 JVM 进行管理，Spark 仅进行逻辑上的“规划式”管理，即对象实例占用内存的申请和释放都由 JVM 完成，Spark 只在申请后和释放前记录这些内存。由于 JVM 的对象可以序列化方式存储，序列化过程为将对象转换为二进制字节流，可理解为将非连续空间的链式存储转化为连续空间或块存储，在访问时则需要进行序列化的逆过程——反序列化，将字节流转化为对象。序列化方式可节省存储空间，但增加了存储和读取时的计算开销。由 SPARK-11389:Add support for off-heap memory to MemoryManager，为进一步优化内存使用及提高 shuffle 时排序效率，Spark 引入了堆外内存，使之能够直接在工作节点的系统内存中开辟空间，存储经过序列化的二进制数据。除没有 other 空间以外，堆外内存与堆内内存的划分方式相同，所有运行中的并发任务共享存储内存和执行内存。

这种模式通过调用 Java 的 unsafe 相关 API 直接向操作系统申请内存，而不是在 JVM内申请，故可以避免频繁的 GC，但需要自行编写内存申请和释放的逻辑。如果堆外内存被启用，则 Executor 中的 Execution memory 内存为堆内和堆外内存之和，Storage 同理。

（2）内存空间分配

1. 动态占用机制——execution & storage

Spark2.x 支持 execution 内存和 storage 内存的相互共享。即若 Execution 内存不足，而 Storage 内存有空闲，则 Execution 可从 Storage 中申请空间，反之亦然。

程序提交时利用 spark.memory.storageFraction 参数设置基本的 Execution 内存和Storage 内存区域

程序运行时，若双方的空间不足，则依照 LRU 规则将内存中的块存储到磁盘；若己方空间不足(即不足以放下一个完整的 block)而对方空余，则借用对方的空间。Execution 内存的空间被对方占用后，可让对方将占用的部分转存到硬盘，然后归还借用的空间。Storage 内存的空间被对方占用后，由于需要考虑 shuffle 过程中的诸多因素，因而实现复杂，且 shuffle 过程中产生的文件在后续一定会被使用而 cache 在内存的数据不一定在后续使用，因此目前 Spark2.x 设定无法让对方归还借用空间。

（3）存储内存管理

1. RDD 持久化

在 Spark 中，当多次对同一个 RDD 执行算子操作时，每一次都会对这个 RDD 的父 RDD重新计算一次，这种重复计算是对资源的极大浪费，因此必须对多次使用的 RDD 进行持久化，通过持久化将公共的 RDD 数据缓存到磁盘/内存中，之后对于公共 RDD 的计算即可直接获取。

RDD 的持久化可进行序列化，当内存无法将 RDD 的数据完整得进行存放的时候，可以考虑使用序列化的方式减小数据体积，将数据完整存储在内存中。若对于数据的可靠性要求很高且内存充足，则可以使用副本机制，对 RDD 数据进行持久化。当持久化启用副本机制，对于持久化的每一个数据单元都将存储一个副本放在其他节点上，从而实现数据的容错。

（4）执行内存管理

1. Shuffle 的内存占用

Shuffle 为按照一定规则对 RDD 数据重新分区的过程，主要分为两个阶段——Shuffle Write 和 Shuffle Read，执行内存主要用于存储任务在执行 Shuffle 时占用的内存。

**Shuffle Write 对执行内存的使用**

1.若在 map 端选择普通的排序方式，会采用 ExternalSorter 进行外排，在内存中存储数据时主要占用堆内执行空间。

2.若在 map 端选择 Tungsten 的排序方式，则采用 ShuffleExternalSorter 直接对以序列化形式存储的数据排序，在内存中存储数据时可以占用堆外或堆内执行空间，取决于用户是否开启了堆外内存以及堆外执行内存是否足够。

**Shuffle Read 对执行内存的使用**

1.在对 reduce 端的数据进行聚合时，要将数据交给 Aggregator 处理，在内存中存储数据时占用堆内执行空间。

2.如果需要进行最终结果排序，则要将再次将数据交给 ExternalSorter 处理，占用堆内执行空间。

(二)Spark 内存优化方案

针对上述分析中 Spark 的内存使用特性，提出了以下四种针对性的解决策略。

策略 1：开辟堆外内存

由于 Spark 的堆外内存默认不开启，因此开辟堆外内存。

策略 2：调整堆内内存比例

根据 SPARK-15796:Reduce spark.memory.fraction default to avoid overrunning oldgen in JVM default config 可知，如果比例设置偏高，则会导致 gc 时间过长。因此将比例调整至 0.6策略 

3：启用 Tungsten 排序方式

Tungsten 通过对执行内存的使用进行抽象，使得在 Shuffle 过程中无需关心数据具体存储在堆内还是堆外。每个内存页用一个 MemoryBlock 来定义，并用 Object obj 和 longoffset 这两个变量统一标识一个内存页在系统内存中的地址。

有了统一的寻址方式后，Spark 可以用 64 位逻辑地址的指针定位到堆内或堆外的内存，因此整个 Shuffle Write 排序的过程只需对指针进行排序，并且无需反序列化，整个过程非常高效，对于内存访问效率和 CPU 使用效率带来了明显的提升。因此启用 Tungsten 排序方式。

策略 4：适当增加或降低 RDD 分区数目

由于 RDD 的分区对应一个 Task 处理数据，开始时数据量较多，在集群资源充足的情况下可以通过增加 RDD 分区数目增加并行度。当数据预处理后，RDD 中数据量减少了很多，且Shuffle 阶段需要的执行空间较大，若将 Execution 分割则很难进行协调，此时可以减少分区数目。

#### SparkSQL

**3.1 spark SQL 简介**

sparkSQL 是一个用于结构化数据处理的 Spark 模块。它提供了一个称为 DataFrames 的编程抽象，还可以充当分布式 SQL 查询引擎。

sparkSQL 简化了数据处理的复杂度，使用它可以省去大量繁琐的非核心工作，使得用户更加专注于数据处理本身。

sparkSQL 在 RDD 上面构建了通用的 SQL 组件，能让用户通过简单的 SQL 语句实现复杂的数据处理逻辑。

**3.2 spark SQL vs spark**

sparkSQL 相对于 spark 而言，会占用相对较少的内存，因为 spark SQL 采用存储的方式，与 Spark 的原生缓存（只将数据存储为 JVM 对象）相比，列式缓存可以将内存占用减少一个数量级，因为它采用了字典编码和运行长度编码等列式压缩方案；而且同样的任务用spark SQL 常常可以更快，因为独立的 SQL 解析器 Catalyst，在这个解析器中有多步对执行的优化。

Catalyst 主要由以下部分组成：

SQLParse：解析 SQL 语法，然后生成一个未解析的逻辑计划

Analyzer：将上一步的逻辑计划和相应的元数据结合起来生成一个解析过的逻辑计划

Optimizer：对上一步的解析过的逻辑计划按照一定的策略进行优化，得到一个优化的逻辑计划

Planner：将上一步优化之后的逻辑计划转换成可执行的物理计划 CostModel：基于代价模

型选择出一个最佳的物理计划，并提交给 Spark 执行。整个流程如图：

**3.3 实验过程**

**（1）数据过滤与规约**

因为我们实验的目的是找到最有可能是伴随车的车对，而且挖掘过程受到服务器内存限制，所以我们首先对数据进行合理地过滤与规约，这样也可以大大提高处理速度。

我们首先把数据按时间戳分成 31 天的数据、然后对去除重复数据（因为只有在车辆伴随次数达到某个阈值时才可判定为伴随车，所以对于出现次数太少的车辆，我们无法从其数据中挖掘出有效的伴随关系，所以可以将这些数据去除。）

考虑到利用伴随车进行伴随犯罪的，绝大多数为团伙作案。根据犯罪心理学，团伙犯罪的一大特征是犯罪动机不稳定，且因为人数较多，具有纠合性和易变性，因而在一个月内频繁犯案的可能性不大。所以我们将其犯案频率定为平均 5 天一次。而根据犯罪地理学，犯人在选择犯罪地点的时候通常会依据“不远，不近，不重复”的原则，因而我们选定某团伙一次作案平均会经过 50 个路口。但是我们的数据是以秒为单位的时间戳，所以车辆在一个路口的一次出现可能会有多条记录。考虑实际情况如路口的平均宽度、车辆经过路口的限速、道口信号配时（绝大多数情况下为 1：1）、红灯长度（红灯在设计时因为考虑了人最大忍耐的等待时间所以最长不会超过 90s，也有很多红灯长度只有 10s 左右），我们将车辆经过路口的平均时间定为 20s，即一辆车在一个路口的一次出现平均会有 20 条记录。因而真正的团伙作案伴随车辆每天伴随次数不会低于 200 次。而车辆出现的次数必定大于等于与其他车伴随的次数，所以我们可以直接去掉一天中出现次数小于 200 的车辆数据。而由于在真正伴随的情况下，去掉少数单个路口不会对结果产生影响，只需要对判定为伴随车的阈值稍加调整即可，所以我们考虑去掉一天中数据条目最多的 5 个路口（路口总共有超过 1000 个，我们去掉的繁华路口只占总路口的不到 0.5%）

过滤规约之后，原来的约 2 亿的数据变为了约 299.2 万。由此可见原始数据长尾化特征明显，需要进行规约处理。这里其实我们在进行数据过滤的时候限定的条件比较严格。如果想得到更多的伴随车对，可以适当放宽条件，使得过滤之后剩下更多的数据。

**（2）spark SQL 代码生成伴随车对**

**①数据读入**

我们用一个序列化的 java 类读保存每条数据（每条数据包含车号、路口号、时间戳 3 个域）。考虑到每个域的数值都在 0-2147483647 之间，所以用 int 型存储每个域的数值。读取过滤后的数据，转换为 spark 的 RDD 类型。然后调用 map 把每一条数据变为上边定义的 crossMonitor 对象，然后把 RDD 转化成 spark SQL 的 dataframe，再创建一张 spark SQL表。

**②车辆伴随情况生成**

和 spark 方法中一样，选定两车经过同一个路口的时间差不超过 60s 即认为伴随一次。为了避免数据重复，即伴随情况结果中出现（1，2）伴随一次，（2，1）伴随一次这样的情况，令 car1 的 ID 一定要小于 car2 的 ID。而这里也体现出数据规约的重要性。如果不进行数据预处理，这里生成伴随情况时对两份原始的数据表做的 join 操作会严重超过服务器内存的承受范围。

**③伴随车筛选**

按照数据规约时的分析，在一个月内伴随次数超过 6000 的车辆才应判定为伴随车。但是由于我们去掉了每天数据条目最多的 5 个路口，所以我们相应适当减小判定伴随车的阈值。考虑到车辆出现在数据条目多的路口的概率比出现在其他路口的概率要大，所以我们此处将判定为伴随车的阈值定为 5600. 由于伴随次数越多的车辆越有可能是真正的伴随车，也就是我们真正关注的车辆，所以我们对挖掘出的伴随车对按伴随次数从大到小进行排序最后代码运行时整个处理过程不超过 3 分钟。且未报有关内存的错误。可见进行数据过滤后用 spark SQL 处理是一种轻便的方法。

**3.4 改进空间**

（1）本过程只抽取出了两辆车组成的伴随车对。而实际情况一个作案团伙可能会有多辆车，也就是存在多辆车伴随的情况。如果想要挖掘这种情况通过简单改写 sql 语句即可实现（这也是 spark SQL 相对于 spark 方法的便捷之处），只是处理难度和内存消耗会增加。

（2）实际应用中还可以考虑更实际的情况。比如由于夜晚的犯罪率比白天高，因而夜晚伴随的车更有可能是伴随车，所以生成伴随情况时可以加一列时间戳，在计算伴随次数的时候对不同的时间的伴随给予不同的权重。再比如由于罪犯选择犯罪地点有随机性和“不重复”的原则，所以我们的伴随情况中可以加上一行路口号，在筛选伴随车的时候倾向于认为总是在相同的几个路口伴随的车不是伴随车（有可能是某单位的两辆班车从家属楼接送员工）

（3）受到 spark 方法的启发，我认为在 spark SQL 方法中我们也可以对 sql 语句进行改进，对 car1 和 car2 的出现次数做 count 操作然后把置信度与阈值进行比较从而剔除随机伴随的情况。

### 5 结语

本次伴随车挖掘项目中，我们共采用了两种方法对超过 2 亿的车辆数据进行了挖掘处理。其中 spark 方法中尝试了 Apriori 和 FP-growth 两种算法。基于 Spark 计算框架利用滑动时间窗口数据结构得到的伴随车辆数据集；再基于 Spark 并行分布式框架的关联规则挖掘算法的伴随车辆发现，得到伴随车辆结果。

之后我们还就运行时遇到的内存不足问题，基于 spark 的内存机制进行了优化。

spark SQL 方法则是利用了特定的 spark 模块。先对数据进行了筛选和规约，再利用 SQL语句筛选出符合条件的伴随车对。





参考文献：

[1]Michael Armbrust ,Spark SQL: Relational Data Processing in Spark

[2]杨卫宁, 邹维宝. 基于Spark的出租车轨迹处理与可视化平台. 计算机系统应用, 2020,29(3): 64-72.http://www.c-s-a.org.cn/1003-3254/7308.html

[3]罗昭， Spark SQL 结构化数据处理及性能优化

[4] 方艾芬，李先通.基于关联规则挖掘的伴随车辆发现算法 [J]. 计算机应用与软件，2012，29（2）：94-96

[5] Jiawei Han，Jian Pei，Yiwen Yin． Mining Frequent Patterns without Candidate Generation［C］/ /Proc．Special Interest Groups on Manage- ment of Data，2000 ( SIGMOD 2000) ，Dallas，Texas，United States． 2000: 1 12

[6]王路辉. 基于车牌流数据的伴随车发现方法研究[D]. 2017.

[7]陈瑶, 桂峰, 卢超,等. 基于频繁项集挖掘算法的伴随车应用与实现[J]. 计算机应用与软件, 2017(4).

[8]刘惠惠, 张祖平, 龙哲. 基于Spark的FP-Growth伴随车辆发现与应用[J]. 计算机工程与应用, 2018, v.54;No.903(08):12-18+40.

[9]SPARK-12081:Make unified memory management work with small heaps

[10]SPARK-11389:Add support for off-heap memory to MemoryManager

[11]SPARK-15796:Reduce spark.memory.fraction default to avoid overrunning old gen in JVM default config

附注：

在整个 hadoop 和 spark 集群搭建、使用过程中我们也遇到了很多问题，这里我们总结了我们的搭建过程、遇到的问题和解决方法，希望可以帮助其他需要使用这些平台的人。

一、Hadoop 搭建与配置

1.主要参考 https://www.jianshu.com/p/dc2913a07770，以下报告只记录一些遇到的问题和注意事项

2.所有的配置设置工作都要远程连接到老师分配的服务器上进行。并在每台服务器上都进行一次。

3.配置用户和用户组的时候要以 root 用户的身份连接到远程的服务器

4.为了防止发生不必要的错误，/etc/hosts 文件中的前两行就不要改动了，直接注释掉就可以

5.分配的服务器是 centos，CentOS 是 Linux 众多得发行版本之一。但其命令行和 ubuntu有所不通过，比如安装要用 yum 命令代替 apt。如 ssh 安装指令 yum list installed | grepopenssh-server

6.在修改文件的时候先按 i 进入 insert 模式，修改之后 esc，因为是只读文件所以输入：wq!才可以成功保存退出

7.修改文件时以 root 用户还是以 hadoop 用户的身份都是一样的，但是配置免密登录的时候一定要以 hadoop 用户的身份进行操作

8.如果配置免密登录失败，可以把 slave1-4 上面的 authorized_keys 都删掉，在 master节点上使用 ssh-copy-id hadoop@slave2 将公钥追加过去

9.防火墙不用特意关闭，用 systemctl status firewalld 命令进行检查的时候会发现防火墙的状态都是 inactive(dead)的。操作防火墙的时候还是要用 root 用户的身份登录

10.CentOS 是 Linux 众多得发行版本之一，所以下载 jdk 的时候要下载 Linux 系统适用的

11.配置 java jdk 和 hadoop 的时候一定注意要以 hadoop 用户的身份连接到服务器，并且在配置的时候不要乱用 sudo，用 sudo 指令就相当于 root 用户了，会导致文件的用户名用户组变成 root。（但是有些命令行执行的时候可能出现问题，这时也可以用 sudo 解决，一般在遇到 permission denied 报错的时候用 sudo 有用）。之后可以用 ls -l 命令查看文件所属的用户名和用户组。如果最后发现 usr/local/cluster/中的某些文件的用户或者用户组不是 hadoop 要手动改为 hadoop，每一台机器上的文件都要进行改动，否则在启动 hdfs 的时候会报 No such file or dictionary 的错误。手动修改用户名：chown -R 想改成的用户名 相应的文件夹名 eg.想把 java 文件夹的用户名改为 hadoop，chown -R hadoop java，如果想改的是文件不是文件夹，就不加-R。手动修改用户组，chgrp [-R] 想改成的用户组名 相应的文件夹名。

12.除了 hadoop-env.sh 和 yarn-env.sh 以外的配置文件在修改的时候要把文件中原来的东西全部删除，否则启动 hdfs 的时候会报格式错误

13.mapred-site.xml 要通过 sudo cp mapred-site.xml.template mapred-site.xml 命令创建才对

14.vim 命令可能用不了，统一用 vi 就可以

15.cat /etc/redhat-release 可以查看 centos 版本

16.如果启动 hdfs 等的时候出现问题，可以到/usr/local/cluster/hadoop/logs 里找相应的 log 文件（注意不是.out 文件）查看错误具体出在哪里。如果出现类似以下的错误java.net.BindException: Portin use: 0.0.0.0:50070，说明端口 50070 被某个进程占用，可以用 sudo netstat -alnp | grep 50070 查看是哪个进程占用了端口，之后用 kill -9 进程号把相应进程杀掉即可。

17.当多次使用/usr/local/cluster/hadoop/bin/hdfs namenode -format 格式化 namenode时，slave 中的 clusterID 不会随之改变，只能手动改变。方法是 find /usr/local/cluster-name VERSION,在 master 上找到 current/VERSION 文件，在各个 slave 上也找到这个文件，把所有的 slave 的这个文件中的 clusterID 改为和 master 中的 clusterID 一样。