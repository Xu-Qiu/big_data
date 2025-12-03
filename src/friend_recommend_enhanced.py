#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Enhanced Friend Recommendation System (PySpark Implementation)

功能：
1. Job1: 计算每个用户的好友数量（degree）
2. Job2: 增强版好友推荐：
   - 排除已有好友
   - 按共同好友数过滤（minMutual）
   - 支持 Top-N 推荐
   - 可选归一化评分： score = mutualCount / (deg(u) + deg(v) + 1)
   - 按 (score desc, mutualCount desc, candidateID asc) 排序输出
3. 结果输出：
   - 文本版推荐结果（适合实验报告查看）
   - JSON 版推荐结果（便于后续可视化与分析）
   - 本地模式下终端彩色预览推荐结果
"""

import argparse
from typing import List, Tuple, Dict

from pyspark.sql import SparkSession
from pyspark import RDD
import heapq
import json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enhanced Friend Recommendation with PySpark"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="HDFS input path of adjacency list, e.g. hdfs:///user/qiu/input/friends.txt",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="HDFS output path for final recommendations, "
             "e.g. hdfs:///user/qiu/output/enhanced_recs",
    )
    parser.add_argument(
        "--degree_output",
        default=None,
        help="HDFS output path for degree file (optional). "
             "Default: <output>_degree",
    )
    parser.add_argument(
        "--json_output",
        default=None,
        help="HDFS output path for JSON recommendations (optional). Default: <output>_json",
    )
    parser.add_argument(
        "--topN",
        type=int,
        default=10,
        help="Max recommendations per user (Top-N). Default: 10",
    )
    parser.add_argument(
        "--minMutual",
        type=int,
        default=2,
        help="Minimum number of mutual friends required. Default: 2",
    )
    parser.add_argument(
        "--disable_normalization",
        action="store_true",
        help="Disable normalized score and use raw mutual friend count.",
    )
    return parser.parse_args()


# -------------------------
# Job1: Degree Computation
# -------------------------

def parse_line(line: str):
    """
    输入一行邻接表，解析成 (user, [friends])。
    格式示例： "0 1,2,3,4"
    """
    line = line.strip()
    if not line:
        return None
    parts = line.split()
    user = int(parts[0])
    if len(parts) == 1 or not parts[1]:
        friends: List[int] = []
    else:
        friends = [int(x) for x in parts[1].split(",") if x]
    return user, friends


def compute_degrees(user_friends_rdd: RDD) -> RDD:
    """
    Job1: 计算每个用户的度数（好友数量）
    :param user_friends_rdd: RDD[(user, [friends])]
    :return: RDD[(user, degree)]
    """
    degrees = user_friends_rdd.map(lambda uf: (uf[0], len(uf[1])))
    return degrees


# -------------------------
# Job2: Enhanced Reco (Map)
# -------------------------

def mapper_generate_pairs(uf: Tuple[int, List[int]]) -> List[Tuple[int, Tuple[int, int]]]:
    """
    输入：(user, [friends])
    输出：
      - 已有好友对： (user, (f, -1))
      - 潜在推荐对（共同好友）：对于 friendList 中的每一对 (a, b)：
            (a, (b, user)) 和 (b, (a, user))
    """
    user, friends = uf
    result: List[Tuple[int, Tuple[int, int]]] = []

    # A. 已有好友对
    for f in friends:
        result.append((user, (f, -1)))

    # B. 潜在推荐对（共同好友）
    n = len(friends)
    for i in range(n):
        for j in range(i + 1, n):
            a = friends[i]
            b = friends[j]
            # a 和 b 通过 user 成为共同好友
            result.append((a, (b, user)))
            result.append((b, (a, user)))

    return result


# -------------------------
# Job2: Enhanced Reco (Reduce)
# -------------------------

def reduce_for_user(
    record: Tuple[int, List[Tuple[int, int]]],
    degrees: Dict[int, int],
    topN: int,
    min_mutual: int,
    enable_normalization: bool,
) -> Tuple[int, List[Tuple[int, float, List[int]]]]:
    """
    对单个用户执行 Reducer 逻辑。

    :param record: (user, iterable[(candidate, mutualFriendID or -1)])
    :param degrees: {user: degree}
    :return: (user, [(candidate, score, [mutual_friends]), ...])
    """
    user, values_iter = record
    values = list(values_iter)

    # candidate -> list(mutualFriends)
    candidate_mutual: Dict[int, List[int]] = {}
    # 已是直接好友的候选集合
    invalid_friends = set()

    for candidate, mutual_friend in values:
        if mutual_friend == -1:
            # 已有好友关系（或自连接），记为无效推荐
            invalid_friends.add(candidate)
        else:
            candidate_mutual.setdefault(candidate, []).append(mutual_friend)

    # 取当前用户度数（若缺失则视为 0）
    deg_u = degrees.get(user, 0)

    candidates_info: List[Tuple[float, int, int, List[int]]] = []

    for candidate, mutual_list in candidate_mutual.items():
        # 跳过已是好友或自己
        if candidate in invalid_friends or candidate == user:
            continue

        mutual_count = len(mutual_list)
        # minMutual 阈值过滤
        if mutual_count < min_mutual:
            continue

        deg_v = degrees.get(candidate, 0)

        if enable_normalization:
            score = float(mutual_count) / float(deg_u + deg_v + 1)
        else:
            score = float(mutual_count)

        candidates_info.append((score, mutual_count, candidate, mutual_list))

    if not candidates_info:
        return user, []

    # 使用 nlargest 实现 Top-N
    # 排序规则（从大到小）:
    #   score 降序, mutualCount 降序, candidateID 升序
    top_candidates = heapq.nlargest(
        topN,
        candidates_info,
        key=lambda x: (x[0], x[1], -x[2])  # candidateID 小的优先，所以取 -candidate
    )

    # 转成 (candidate, score, mutual_list) 形式
    result = [(cand, score, mutuals) for (score, mutual_count, cand, mutuals) in top_candidates]

    return user, result


def format_output_line(user: int, recs: List[Tuple[int, float, List[int]]]) -> str:
    """
    输出更漂亮的格式：
    User X    (no recommendations)
    User X    Recommend Y    Score=0.4286    Mutual=3    MutualFriends=[...]
    """
    if not recs:
        return f"User {user}\t(no recommendations)"

    lines = []
    for cand, score, mutuals in recs:
        mutuals_sorted = sorted(mutuals)
        line = (
            f"User {user}\t"
            f"Recommend {cand}\t"
            f"Score={score:.4f}\t"
            f"Mutual={len(mutuals_sorted)}\t"
            f"MutualFriends={mutuals_sorted}"
        )
        lines.append(line)

    # 推荐每个用户多行输出
    return "\n".join(lines)

# 彩色文本格式（只在 local 模式下可见）
class Color:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    GRAY = '\033[90m'
    RESET = '\033[0m'


def format_colored_output(user: int, recs: List[Tuple[int, float, List[int]]]) -> str:
    """
    同样的输出，但带颜色（用于本地展示）
    """
    if not recs:
        return f"{Color.BLUE}User {user}{Color.RESET}  {Color.GRAY}(no recommendations){Color.RESET}"

    lines = []
    for cand, score, mutuals in recs:
        mutuals_sorted = sorted(mutuals)
        line = (
            f"{Color.BLUE}User {user}{Color.RESET}  "
            f"{Color.GREEN}Recommend {cand}{Color.RESET}  "
            f"{Color.YELLOW}Score={score:.4f}{Color.RESET}  "
            f"Mutual={len(mutuals_sorted)}  "
            f"MutualFriends={mutuals_sorted}"
        )
        lines.append(line)
    return "\n".join(lines)


def to_json_record(user: int, recs: List[Tuple[int, float, List[int]]]) -> str:
    """将 (user, recs) 转成一行 JSON 字符串，用于 saveAsTextFile。"""
    rec_list = []
    for cand, score, mutuals in recs:
        mutuals_sorted = sorted(mutuals)
        rec_list.append(
            {
                "candidate": cand,
                "score": float(f"{score:.4f}"),
                "mutual": len(mutuals_sorted),
                "mutual_friends": mutuals_sorted,
            }
        )
    record = {"user": user, "recommendations": rec_list}
    return json.dumps(record, ensure_ascii=False)


def main():
    args = parse_args()

    input_path = args.input
    output_path = args.output
    degree_output_path = args.degree_output or (output_path.rstrip("/") + "_degree")
    json_output_path = args.json_output or (output_path.rstrip("/") + "_json")

    topN = args.topN
    min_mutual = args.minMutual
    enable_normalization = not args.disable_normalization

    spark = (
        SparkSession.builder
        .appName("EnhancedFriendRecommend")
        .master("local[*]")  # 你可以改成 "yarn" 等
        .getOrCreate()
    )
    sc = spark.sparkContext

    print("=== Configuration ===")
    print(f"Input path         : {input_path}")
    print(f"Degree output path : {degree_output_path}")
    print(f"Reco output path   : {output_path}")
    print(f"JSON output path   : {json_output_path}")
    print(f"topN               : {topN}")
    print(f"minMutual          : {min_mutual}")
    print(f"enableNormalization: {enable_normalization}")
    print("=====================")

    # 读取邻接表
    lines = sc.textFile(input_path)

    # 解析成 (user, [friends])
    user_friends_rdd = (
        lines
        .map(parse_line)
        .filter(lambda x: x is not None)
    )

    # ----------------
    # Job1: Degrees
    # ----------------
    degrees_rdd = compute_degrees(user_friends_rdd)

    # 保存 Degree 文件（Job1 输出）
    degrees_rdd.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(degree_output_path)

    # 收集到 Driver 端并广播（模拟 DistributedCache）
    degrees_dict = dict(degrees_rdd.collect())
    degrees_bc = sc.broadcast(degrees_dict)

    # ----------------
    # Job2: Enhanced Reco
    # ----------------

    # Mapper: 生成已有好友对 + 潜在推荐对
    pair_rdd = user_friends_rdd.flatMap(mapper_generate_pairs)
    # pair_rdd: RDD[(user, (candidate, mutualFriendID or -1))]

    # Reduce：对每个 user 聚合并计算 Top-N 推荐
    grouped_rdd = pair_rdd.groupByKey()

    enhanced_reco_rdd: RDD = grouped_rdd.map(
        lambda kv: reduce_for_user(
            kv,
            degrees=degrees_bc.value,
            topN=topN,
            min_mutual=min_mutual,
            enable_normalization=enable_normalization,
        )
    )

    # 生成 JSON 版本推荐结果
    json_lines_rdd = enhanced_reco_rdd.map(
        lambda kv: to_json_record(kv[0], kv[1])
    )
    json_lines_rdd.saveAsTextFile(json_output_path)

    # 格式化成最终输出行
    output_lines_rdd = enhanced_reco_rdd.map(
        lambda kv: format_output_line(kv[0], kv[1])
    )

    # 保存到 HDFS
    output_lines_rdd.saveAsTextFile(output_path)

    # 如果在本地模式，额外打印彩色预览（仅适合小数据量）
    if sc.master and sc.master.startswith("local"):
        print("\n=== Colored recommendations preview (local) ===")
        preview_records = enhanced_reco_rdd.take(50)
        for user, recs in preview_records:
            print(format_colored_output(user, recs))
        print("=== End of preview ===\n")

    print("Job finished. Results written to:")
    print(f"  Degrees     : {degree_output_path}")
    print(f"  Recommends  : {output_path}")
    print(f"  JSON        : {json_output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
