import pandas as pd
from pyspark.sql import SparkSession
import time
import psutil
import os
from pathlib import Path
import matplotlib.pyplot as plt
import random
import numpy as np

random.seed(42)
np.random.seed(42)



# Java 11
os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-11.0.26.4-hotspot"
os.environ["HADOOP_HOME"] = "C:/Program Files/hadoop"  

# ========== Configurações ==========
CSV_FILE = "BigData/augmentation_datasets/fastfood_augmented.csv"
SAMPLES = [1000, 5000, 10000, 12500, 17500, 20000]
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "benchmarks"
OUTPUT_FILE = OUTPUT_DIR / "benchmark_results.csv"
CHART_TIME = OUTPUT_DIR / "execution_time.png"
CHART_MEM = OUTPUT_DIR / "memory_usage.png"

# Criar diretório de benchmarks se não existir
OUTPUT_DIR.mkdir(exist_ok=True)

# ========= Funções de monitorização =========
def memory_usage_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # em MB

def measure_python(sample_size):
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()

    df = pd.read_csv(CSV_FILE)
    df = df.sample(n=sample_size, random_state=42)
    result = df.groupby("state").count()

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

def measure_pyspark(spark, sample_size):
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()

    df = spark.read.csv(CSV_FILE, header=True, inferSchema=True)
    total_rows = df.count()
    frac = sample_size / total_rows
    df_sample = df.sample(withReplacement=False, fraction=frac, seed=42)
    result = df_sample.groupBy("state").count()
    result.collect()

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

# ========= Iniciar Spark =========
spark = SparkSession.builder \
    .appName("BenchmarkPythonVsPySpark") \
    .master("local[*]") \
    .getOrCreate()

# ========= Cabeçalho do CSV =========
if not OUTPUT_FILE.exists():
    with open(OUTPUT_FILE, "w") as f:
        f.write("engine,sample_size,time_sec,memory_mb\n")

# ========= Loop de benchmarks =========
for size in SAMPLES:
    print(f"\n🔍 Benchmark para sample size = {size}")

    # Python (pandas)
    time_py, mem_py = measure_python(size)
    print(f"[Python] ⏱ {time_py}s | 💾 {mem_py}MB")
    with open(OUTPUT_FILE, "a") as f:
        f.write(f"python,{size},{time_py},{mem_py}\n")

    # PySpark
    time_sp, mem_sp = measure_pyspark(spark, size)
    print(f"[PySpark] ⏱ {time_sp}s | 💾 {mem_sp}MB")
    with open(OUTPUT_FILE, "a") as f:
        f.write(f"pyspark,{size},{time_sp},{mem_sp}\n")

# ========= Fechar sessão Spark =========
spark.stop()

print("\n📊 A gerar gráficos...")

# ========= Gerar Gráficos =========
df = pd.read_csv(OUTPUT_FILE)

# Tempo de Execução
plt.figure(figsize=(10, 6))
for engine in df["engine"].unique():
    subset = df[df["engine"] == engine]
    plt.plot(subset["sample_size"], subset["time_sec"], marker="o", label=engine)
plt.title("Tempo de Execução - Python vs PySpark")
plt.xlabel("Sample Size")
plt.ylabel("Tempo (segundos)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig(CHART_TIME)
plt.close()

# Uso de Memória
plt.figure(figsize=(10, 6))
for engine in df["engine"].unique():
    subset = df[df["engine"] == engine]
    plt.plot(subset["sample_size"], subset["memory_mb"], marker="o", label=engine)
plt.title("Uso de Memória - Python vs PySpark")
plt.xlabel("Sample Size")
plt.ylabel("Memória (MB)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig(CHART_MEM)
plt.close()

print("✅ Gráficos salvos em:")
print(f"   📁 {CHART_TIME}")
print(f"   📁 {CHART_MEM}")
print(f"📄 Resultados CSV: {OUTPUT_FILE}")
