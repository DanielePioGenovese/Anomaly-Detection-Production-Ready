# Spark Configuration Examples
# Different configurations for different use cases

"""
Choose the appropriate configuration based on:
- Dataset size
- Available memory
- Local vs cluster execution
"""

from pyspark.sql import SparkSession

# ============================================================================
# 1. LOCAL DEVELOPMENT (Small datasets: 10K-100K rows)
# ============================================================================

def get_spark_local_dev():
    """
    For quick testing and development
    Memory: 2GB driver
    """
    return SparkSession.builder \
        .appName("Washer Generator - Dev") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()


# ============================================================================
# 2. LOCAL PRODUCTION (1M rows)
# ============================================================================

def get_spark_local_prod():
    """
    For generating full 1M row dataset locally
    Memory: 4GB driver
    Uses all available cores
    """
    return SparkSession.builder \
        .appName("Washer Generator - Production") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


# ============================================================================
# 3. HIGH MEMORY (Large datasets: 5M+ rows)
# ============================================================================

def get_spark_high_memory():
    """
    For very large datasets
    Memory: 8GB driver
    Optimized for memory-intensive operations
    """
    return SparkSession.builder \
        .appName("Washer Generator - High Memory") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.default.parallelism", "400") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


# ============================================================================
# 4. CLUSTER MODE (Distributed processing)
# ============================================================================

def get_spark_cluster(master_url="spark://master:7077"):
    """
    For cluster deployment
    Adjust resources based on cluster capacity
    
    Parameters:
    -----------
    master_url : str
        Spark master URL
    """
    return SparkSession.builder \
        .appName("Washer Generator - Cluster") \
        .master(master_url) \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "10") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .getOrCreate()


# ============================================================================
# 5. DATABRICKS CONFIGURATION
# ============================================================================

def get_spark_databricks():
    """
    For Databricks environment
    Databricks manages most configs automatically
    """
    return SparkSession.builder \
        .appName("Washer Generator - Databricks") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


# ============================================================================
# 6. AWS EMR CONFIGURATION
# ============================================================================

def get_spark_emr():
    """
    For AWS EMR cluster
    Optimized for S3 I/O
    """
    return SparkSession.builder \
        .appName("Washer Generator - EMR") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


# ============================================================================
# 7. MINIMAL MEMORY (Constrained environments: <2GB RAM)
# ============================================================================

def get_spark_minimal():
    """
    For environments with limited memory
    Generate smaller batches and write incrementally
    """
    return SparkSession.builder \
        .appName("Washer Generator - Minimal") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "20") \
        .config("spark.ui.enabled", "false") \
        .config("spark.memory.fraction", "0.6") \
        .getOrCreate()


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    
    print("\nAvailable Spark configurations:")
    print("================================\n")
    
    configs = [
        ("Local Development", "get_spark_local_dev", "Quick testing (2GB)"),
        ("Local Production", "get_spark_local_prod", "Full 1M rows (4GB)"),
        ("High Memory", "get_spark_high_memory", "Large datasets (8GB)"),
        ("Cluster", "get_spark_cluster", "Distributed processing"),
        ("Databricks", "get_spark_databricks", "Databricks platform"),
        ("AWS EMR", "get_spark_emr", "AWS EMR cluster"),
        ("Minimal Memory", "get_spark_minimal", "Limited RAM (<2GB)")
    ]
    
    for i, (name, func, desc) in enumerate(configs, 1):
        print(f"{i}. {name:20s} - {desc}")
    
    print("\nUsage:")
    print("------")
    print("from spark_configs import get_spark_local_prod")
    print("spark = get_spark_local_prod()")
    print("\nOr modify the generator to use your preferred config:")
    print("from industrial_washer_generator import generate_industrial_washer_datasets")
    print("normal_df, anomaly_df = generate_industrial_washer_datasets(spark, num_rows=1_000_000)")