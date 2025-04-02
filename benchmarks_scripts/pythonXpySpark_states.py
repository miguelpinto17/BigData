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

# force the exec to be at level of the current file
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ========== Configurações ==========
DATA_DIR = "../county_partitioned_states"
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

def measure_python():
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()
    dfs = [pd.read_csv(os.path.join(DATA_DIR, file)) for file in os.listdir(DATA_DIR) if file.endswith(".csv")]
    df_pandas = pd.concat(dfs, ignore_index=True)
    result = df_pandas.groupby("State").count()

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

def measure_pyspark():
    start_mem = memory_usage_mb()
    start_time = time.perf_counter()
    df_spark = spark.read.option("header", True).csv(os.path.join(DATA_DIR, "*.csv"))
    result = df_spark.groupBy("State").count()
    result.collect()  # Força a execução

    end_time = time.perf_counter()
    end_mem = memory_usage_mb()

    return round(end_time - start_time, 4), round(end_mem - start_mem, 2)

# ========= Iniciar Spark (SEM Hadoop) =========
spark = SparkSession.builder \
    .appName("BenchmarkPythonVsPySpark") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/tmp") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=\"C:/Program Files/Java/jdk-XX/bin\"") \
    .config("spark.executor.extraJavaOptions", "-Djava.library.path=\"C:/Program Files/Java/jdk-XX/bin\"") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()


# ========= Carregar dados ========= 
# Pandas 
#dfs = [pd.read_csv(os.path.join(DATA_DIR, file)) for file in os.listdir(DATA_DIR) if file.endswith(".csv")]
#df_pandas = pd.concat(dfs, ignore_index=True)

# PySpark 
# df_spark = spark.read.option("header", True).csv(os.path.join(DATA_DIR, "*.csv"))

# ========= Executar benchmarks ========= 
print("\n🔍 Executando benchmarks...")

# importing datasets to memory
#TODO
# concat de tds 
time_py, mem_py = measure_python() #df_pandas)
time_sp, mem_sp = measure_pyspark() #spark, df_spark)

print(f"[Python] {time_py} | {mem_py}MB")
print(f"[PySpark] {time_sp} |{mem_sp}MB")

# ========= Salvar resultados =========
with open(OUTPUT_FILE, "w") as f:
    f.write("engine,time_sec,memory_mb\n")
    f.write(f"python,{time_py},{mem_py}\n")
    f.write(f"pyspark,{time_sp},{mem_sp}\n")

# ========= Fechar sessão Spark =========
spark.stop()

print("\n📊 Gerando gráficos...")

# ========= Gerar Gráficos =========
df = pd.read_csv(OUTPUT_FILE)

plt.figure(figsize=(8, 5))
plt.bar(df["engine"], df["time_sec"], color=["blue", "orange"])
plt.title("Tempo de Execução - Python vs PySpark")
plt.ylabel("Tempo (segundos)")
plt.savefig(CHART_TIME)
plt.close()

plt.figure(figsize=(8, 5))
plt.bar(df["engine"], df["memory_mb"], color=["blue", "orange"])
plt.title("Uso de Memória - Python vs PySpark")
plt.ylabel("Memória (MB)")
plt.savefig(CHART_MEM)
plt.close()

print("✅ Gráficos gerados!")



