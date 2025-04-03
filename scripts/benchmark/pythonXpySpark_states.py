import pandas as pd
from pyspark.sql import SparkSession
import time
import psutil
import os
from pathlib import Path
import matplotlib.pyplot as plt

# Forçar execução relativa ao local do script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ========== Configurações ==========
DATA_DIR = Path("../../data/county_partitioned_states")
OUTPUT_DIR = Path("../../results")
OUTPUT_FILE = OUTPUT_DIR / "benchmark_county_results.csv"
CHART_TIME = OUTPUT_DIR / "time_analysis_county.png"
CHART_MEM = OUTPUT_DIR / "memory_analysis_county.png"

# Criar diretório de resultados se não existir
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ========= Funções de monitorização =========
def memory_usage_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # em MB

def measure_python(df):
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()

    result = df.groupby("State").count()

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

def measure_pyspark(spark, df):
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()

    result = df.groupBy("State").count()
    result.collect()

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

# ========= Iniciar Spark =========
spark = SparkSession.builder \
    .appName("BenchmarkPythonVsPySpark") \
    .master("local[*]") \
    .getOrCreate()

# ========= Carregar dados =========
# Pandas
dfs = [pd.read_csv(file) for file in DATA_DIR.glob("*.csv")]
df_pandas = pd.concat(dfs, ignore_index=True)

# PySpark
df_spark = spark.read.option("header", True).csv(str(DATA_DIR / "*.csv"))

# ========= Executar benchmarks =========
print("\n🔍 Executando benchmarks...")

time_py, mem_py = measure_python(df_pandas)
time_sp, mem_sp = measure_pyspark(spark, df_spark)

print(f"[Python] ⏱ {time_py}s | 💾 {mem_py}MB")
print(f"[PySpark] ⏱ {time_sp}s | 💾 {mem_sp}MB")

# ========= Salvar resultados =========
with open(OUTPUT_FILE, "w") as f:
    f.write("engine,time_sec,memory_mb\n")
    f.write(f"python,{time_py},{mem_py}\n")
    f.write(f"pyspark,{time_sp},{mem_sp}\n")

# ========= Fechar sessão Spark =========
spark.stop()

print("\n📊 A gerar gráficos...")

# ========= Gerar Gráficos =========
df = pd.read_csv(OUTPUT_FILE)

# Tempo
plt.figure(figsize=(8, 5))
plt.bar(df["engine"], df["time_sec"], color=["blue", "orange"])
plt.title("Tempo de Execução - Python vs PySpark")
plt.ylabel("Tempo (segundos)")
plt.tight_layout()
plt.savefig(CHART_TIME)
plt.close()

# Memória
plt.figure(figsize=(8, 5))
plt.bar(df["engine"], df["memory_mb"], color=["blue", "orange"])
plt.title("Uso de Memória - Python vs PySpark")
plt.ylabel("Memória (MB)")
plt.tight_layout()
plt.savefig(CHART_MEM)
plt.close()

print("✅ Gráficos gerados em:")
print(f"   📁 {CHART_TIME}")
print(f"   📁 {CHART_MEM}")
print(f"📄 CSV: {OUTPUT_FILE}")
