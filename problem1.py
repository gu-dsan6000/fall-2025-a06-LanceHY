import argparse, csv, os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

LEVELS = ("INFO", "WARN", "ERROR", "DEBUG")
TS_RE   = r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
LVL_RE  = r"(INFO|WARN|ERROR|DEBUG)"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("master_url")
    p.add_argument("--net-id", required=True)
    a = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("Problem1")
        .master(a.master_url)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"s3a://{a.net_id}-assignment-spark-cluster-logs/data/**"

    df = spark.read.text(input_path).select(
        F.regexp_extract("value", TS_RE, 1).alias("ts"),
        F.regexp_extract("value", LVL_RE, 1).alias("log_level"),
        F.col("value").alias("log_entry")
    )
    with_levels = df.where(F.col("log_level") != "").cache()

    total_lines = df.count()
    total_with  = with_levels.count()

    counts = {r["log_level"]: int(r["cnt"]) for r in
              with_levels.groupBy("log_level").agg(F.count("*").alias("cnt")).collect()}
    for k in LEVELS: counts.setdefault(k, 0)

    with open("problem1_counts.csv","w",newline="",encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["log_level","count"])
        for k in LEVELS: w.writerow([k, counts[k]])

    sample = with_levels.select("log_entry","log_level").orderBy(F.rand()).limit(10).collect()
    with open("problem1_sample.csv","w",newline="",encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["log_entry","log_level"])
        for r in sample:
            w.writerow([(r["log_entry"] or "").replace("\r"," ").replace("\n"," "), r["log_level"]])

    denom = max(total_with, 1)
    pct = lambda c: 100.0 * c / denom
    lines = [
        f"Run timestamp (UTC): {datetime.utcnow():%Y-%m-%d %H:%M:%S}",
        f"Input path: {input_path}",
        "",
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_with:,}",
        f"Unique log levels found: {sum(1 for k in LEVELS if counts[k]>0)}",
        "",
        "Log level distribution:"
    ] + [f"  {k:<5}: {counts[k]:>10,} ({pct(counts[k]):5.2f}%)" for k in LEVELS]

    with open("problem1_summary.txt","w",encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
