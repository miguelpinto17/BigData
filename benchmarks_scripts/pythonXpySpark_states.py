import pandas as pd
from pyspark.sql import SparkSession
import time
import psutil
import os
from pathlib import Path
import matplotlib.pyplot as plt

# Java 11
#os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-11.0.26.4-hotspot"
#os.environ["HADOOP_HOME"] = "C:/Program Files/hadoop"  

# ========== Configurações ==========
DATA_DIR = "county_partitioned_states"
SAMPLES = [1000, 5000, 10000, 20000]
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "benchmarks"
OUTPUT_FILE = OUTPUT_DIR / "benchmark_county_results.csv"
CHART_TIME = OUTPUT_DIR / "time_analysis_county.png"
CHART_MEM = OUTPUT_DIR / "memory_analysis_county.png"

# Criar diretório de benchmarks se não existir
OUTPUT_DIR.mkdir(exist_ok=True)

# ========= Funções de monitorização =========
def memory_usage_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # em MB

def measure_python(file, sample_size):
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()

    df = pd.read_csv(os.path.join(DATA_DIR, file))
    sample_size = min(sample_size, len(df))  # Garante que a sample não ultrapasse o total de linhas
    df = df.sample(n=sample_size, random_state=42)
    result = df.groupby("State").count()

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

def measure_pyspark(spark, file, sample_size):
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()

    df = spark.read.csv(os.path.join(DATA_DIR, file), header=True, inferSchema=True)
    total_rows = df.count()
    frac = min(1.0, sample_size / total_rows)  # Garante que frac nunca seja maior que 1
    df_sample = df.sample(withReplacement=False, fraction=frac, seed=42)
    result = df_sample.groupBy("State").count()
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
results = []
files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

for size in SAMPLES:
    print(f"\n🔍 Benchmark para sample size = {size}")
    
    times_py, mems_py, times_sp, mems_sp = [], [], [], []
    
    for file in files:
        # Python (pandas)
        time_py, mem_py = measure_python(file, size)
        times_py.append(time_py)
        mems_py.append(mem_py)

        # PySpark
        time_sp, mem_sp = measure_pyspark(spark, file, size)
        times_sp.append(time_sp)
        mems_sp.append(mem_sp)
    
    avg_time_py, avg_mem_py = round(sum(times_py) / len(times_py), 4), round(sum(mems_py) / len(mems_py), 2)
    avg_time_sp, avg_mem_sp = round(sum(times_sp) / len(times_sp), 4), round(sum(mems_sp) / len(mems_sp), 2)
    
    print(f"[Python] ⏱ {avg_time_py}s | 💾 {avg_mem_py}MB")
    print(f"[PySpark] ⏱ {avg_time_sp}s | 💾 {avg_mem_sp}MB")
    
    with open(OUTPUT_FILE, "a") as f:
        f.write(f"python,{size},{avg_time_py},{avg_mem_py}\n")
        f.write(f"pyspark,{size},{avg_time_sp},{avg_mem_sp}\n")

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
