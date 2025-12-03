# Enhanced Friend Recommendation (PySpark)

PySpark 实验代码，用于在 Hadoop/Spark 上实现基于共同好友的好友推荐。算法与实验细节见 `/Users/qiu/Downloads/big_data_final.pdf`，本仓库用于复现和提交到 GitHub 的代码与示例数据。

## 代码结构
- `src/friend_recommend_enhanced.py`：核心 PySpark 作业，包含度数统计和 Top-N 推荐两个 Job。
- `input/`：示例邻接表数据（小型 `friends.txt` 与较大的 LiveJournal `lj.txt`）。
- `output/`：已跑过的示例输出（文本、JSON、degree）。

## 环境要求
- Python 3
- Apache Spark（测试于 Spark 3.x）及 PySpark
- Hadoop/YARN 集群可选（本地模式也可运行）

## 输入格式
邻接表按行存储：`user_id<空格或制表符>friend1,friend2,...`。例如：
```
0 1,2,3,4
1 0,2,4
```

## 运行方式
使用 `spark-submit`（根据需要切换 master）。

### 本地模式（示例小数据）
```
spark-submit \
  --master local[*] \
  src/friend_recommend_enhanced.py \
  --input input/friends.txt \
  --output /tmp/recs_demo \
  --topN 3 \
  --minMutual 1
```

### Hadoop/YARN 模式（示例 LiveJournal）
```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  src/friend_recommend_enhanced.py \
  --input hdfs:///user/qiu/input/lj.txt \
  --output hdfs:///user/qiu/output/lj_recs \
  --degree_output hdfs:///user/qiu/output/lj_recs_degree \
  --json_output hdfs:///user/qiu/output/lj_recs_json \
  --topN 10 \
  --minMutual 2
```
> 注意：Spark 输出目录必须不存在，如重复运行需要先删除：`hdfs dfs -rm -r hdfs:///user/qiu/output/lj_recs*`

## 参数说明
- `--input`：HDFS/本地邻接表路径（必填）
- `--output`：最终推荐结果目录（文本版，必填）
- `--degree_output`：度数文件目录（可选，默认 `<output>_degree`）
- `--json_output`：JSON 版推荐目录（可选，默认 `<output>_json`）
- `--topN`：每个用户最多返回的候选数，默认 10
- `--minMutual`：最少共同好友数阈值，默认 2
- `--disable_normalization`：使用原始共同好友数作为分值（默认开启归一化：`mutual / (deg(u)+deg(v)+1)`）

## 输出说明
作业会写出三个目录：
1) 度数文件：`<degree_output>`，格式 `user<TAB>degree`
2) 推荐文本：`<output>`，格式类似  
   `User 17636	Recommend 23520	Score=0.1358	Mutual=22	MutualFriends=[...]`
3) 推荐 JSON：`<json_output>`，每行一个用户的 JSON 记录，便于可视化与后处理。

在 `--master local[*]` 时，程序还会在控制台输出彩色预览，便于快速检查结果。

## 原理简述
1) Job1：统计每个用户好友数（degree），并广播到 Reducer。
2) Job2：  
   - Mapper 产生已有好友标记与共同好友候选对。  
   - Reducer 聚合后过滤已有好友/自连接和 `minMutual` 阈值。  
   - 计算分值（可选归一化），按 `(score desc, mutual desc, candidate asc)` 取 Top-N。  
   - 生成文本与 JSON 两种输出。

## 复现实验
- 本地快速验证可用 `input/friends.txt`，观察 `/tmp/recs_demo` 输出。
- 论文/课程实验可直接用 `input/lj.txt` 作为 HDFS 输入，输出示例已在 `output/` 目录中供参考。

## 资料
- 代码入口：`src/friend_recommend_enhanced.py`
