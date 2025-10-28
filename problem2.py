import os
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, regexp_extract, col, expr,
    min as spark_min, max as spark_max, count as spark_count
)

def create_spark_session(master_url):
    spark = (
        SparkSession.builder
        .appName("Problem2")
        .master(master_url)
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    return spark

def main():
    master_url = sys.argv[1]
    net_id = sys.argv[3]

    spark = create_spark_session(master_url)
    path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/**"

    logs_df = spark.read.text(path)

    df = logs_df.withColumn("file_path", input_file_name())
    df = df.withColumn("application_id", regexp_extract("file_path", r"(application_\d+_\d+)", 1))
    df = df.withColumn("cluster_id", regexp_extract("application_id", r"application_(\d+)_\d+", 1))
    df = df.withColumn("ts_raw", regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1))

    df = df.withColumn(
            "timestamp",
            expr("CASE WHEN ts_raw <> '' THEN try_to_timestamp(ts_raw, 'yy/MM/dd HH:mm:ss') END")
        ) \
        .filter(col("timestamp").isNotNull()) \
        .select("cluster_id", "application_id", "timestamp") \
        .cache()

    print("Extracted ID and timestamps")

    df_time = (
        df.groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("application_id")
    )
    df_time_pd = df_time.toPandas()
    df_time_pd["app_number"] = df_time_pd.groupby("cluster_id").cumcount() + 1
    df_time_pd["app_number"] = df_time_pd["app_number"].apply(lambda x: f"{x:04d}")
    os.makedirs("data/output", exist_ok=True)
    df_time_pd.to_csv("data/output/problem2_timeline.csv", index=False)
    print("Generated timeline CSV")


    df_cluster = (
        df_time.groupBy("cluster_id")
        .agg(
            spark_count("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        )
        .orderBy(col("num_applications").desc())
    )
    df_cluster_pd = df_cluster.toPandas()
    df_cluster_pd.to_csv("data/output/problem2_cluster_summary.csv", index=False)
    print("Generated cluster summary CSV")


    total_clusters = df_cluster_pd.shape[0]
    total_apps = df_time_pd.shape[0]
    avg_apps = total_apps / total_clusters if total_clusters else 0

    summary = f"""
    Total unique clusters: {total_clusters}
    Total applications: {total_apps}
    Average applications per cluster: {avg_apps:.2f}
    Most heavily used clusters:
    """
    for _, row in df_cluster_pd.iterrows():
        summary += f"Cluster {row['cluster_id']}: {row['num_applications']} applications\n"

    with open("data/output/problem2_stats.txt", "w") as f:
        f.write(summary.strip())
    print("Wrote summary statistics")


    sns.barplot(x="cluster_id", y="num_applications", data=df_cluster_pd, palette="viridis")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.title("Number of Applications per Cluster")
    plt.tight_layout()
    plt.savefig("data/output/problem2_bar_chart.png")
    print("Bar chart saved")


    largest_cluster = df_cluster_pd.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
    subset = df_time_pd[df_time_pd["cluster_id"] == largest_cluster].copy()
    subset["start_time"] = pd.to_datetime(subset["start_time"])
    subset["end_time"] = pd.to_datetime(subset["end_time"])
    subset["duration_min"] = (subset["end_time"] - subset["start_time"]).dt.total_seconds()/60

    sns.histplot(subset["duration_min"])
    plt.xscale("log")
    plt.xlabel("Application Duration (minutes, log scale)")
    plt.ylabel("Frequency")
    plt.title(f"Application Duration Distribution for Cluster {largest_cluster} (n={len(subset)})")
    plt.tight_layout()
    plt.savefig("data/output/problem2_density_plot.png")
    print("Density plot saved")

    spark.stop()


if __name__ == "__main__":
    main()
