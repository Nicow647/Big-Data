import os
import glob
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATALAKE_DIR = SCRIPT_DIR if os.path.exists(os.path.join(SCRIPT_DIR, "data")) else os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
PREPARED_DATA_DIR = os.path.join(DATALAKE_DIR, "data", "prepared")
# Ruta Gold 
GOLD_DATA_DIR = os.path.join(DATALAKE_DIR, "data", "gold")

spark = (
    SparkSession.builder
    .appName("Lakehouse_Analytics_Gold")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*75)
print("     FASE ANALÍTICA DATA LAKEHOUSE (ZONA GOLD / SERVING LAYER)     ")
print("="*75)

patron = os.path.join(PREPARED_DATA_DIR, "*.parquet")
carpetas_parquet = glob.glob(patron)

if not carpetas_parquet:
    print(f"[ERROR] No se encontraron carpetas Parquet en: {PREPARED_DATA_DIR}")
    spark.stop()
    sys.exit(1)

tabla_parquet = max(carpetas_parquet, key=os.path.getctime)
print(f"[INFO] Leyendo tabla Parquet desde: {tabla_parquet}")

df = spark.read.parquet(tabla_parquet)
df = df.withColumn("hour", F.hour(F.col("event_time")))

print(f"[OK] Carga finalizada. Total registros: {df.count():,}\n")

# CONSULTA 1: Embudo de Conversión
print(">>> [CONSULTA 1] Embudo de Conversión y Abandono por Marca (Top 10)")
funnel_df = df.groupBy("brand").agg(
    F.count(F.when(F.col("event_type") == "view", 1)).alias("total_vistas"),
    F.count(F.when(F.col("event_type") == "cart", 1)).alias("total_carritos"),
    F.count(F.when(F.col("event_type") == "purchase", 1)).alias("total_compras")
).filter((F.col("brand") != "Desconocido") & (F.col("total_vistas") >= 50))

funnel_metricas = (
    funnel_df
    .withColumn("tasa_abandono_carrito_%", 
                F.when(F.col("total_carritos") > F.col("total_compras"),
                       F.round(((F.col("total_carritos") - F.col("total_compras")) / F.col("total_carritos")) * 100, 2))
                .otherwise(0.0))
    .withColumn("conversion_efectiva_%", 
                F.when(F.col("total_vistas") > 0,
                       F.round((F.col("total_compras") / F.col("total_vistas")) * 100, 2))
                .otherwise(0.0))
    .orderBy(F.desc("total_compras"))
)
funnel_metricas.show(10, truncate=False)

# CONSULTA 2: Ranking por Categoría (Window Function)
print("\n>>> [CONSULTA 2] Top 3 Productos por Categoría (DENSE_RANK)")
purchases_df = df.filter(F.col("event_type") == "purchase")
product_sales = purchases_df.groupBy("category_id", "product_id").agg(
    F.count("product_id").alias("unidades_vendidas"),
    F.round(F.sum("price"), 2).alias("facturacion_total")
)
window_spec = Window.partitionBy("category_id").orderBy(F.desc("facturacion_total"))
top_productos = (
    product_sales
    .withColumn("ranking_categoria", F.dense_rank().over(window_spec))
    .filter(F.col("ranking_categoria") <= 3)
    .orderBy("category_id", "ranking_categoria")
)
top_productos.show(15, truncate=False)

# CONSULTA 3: RFM
print("\n>>> [CONSULTA 3] Clientes de Mayor Valor (RFM)")
rfm_df = purchases_df.groupBy("user_id").agg(
    F.max("event_time").alias("ultima_compra"),
    F.count("product_id").alias("frecuencia_compras"),
    F.round(F.sum("price"), 2).alias("gasto_acumulado"),
    F.round(F.avg("price"), 2).alias("ticket_promedio")
).filter(F.col("frecuencia_compras") >= 2).orderBy(F.desc("gasto_acumulado"))
rfm_df.show(10, truncate=False)

# CONSULTA 4: Temporal
print("\n>>> [CONSULTA 4] Distribución Horaria del Tráfico y Facturación")
hourly_df = df.groupBy("hour").agg(
    F.count(F.when(F.col("event_type") == "view", 1)).alias("vistas"),
    F.count(F.when(F.col("event_type") == "purchase", 1)).alias("compras"),
    F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)), 2).alias("ingresos")
).orderBy("hour")
hourly_df.show(24, truncate=False)

# CONSULTA 5: Fricción
print("\n>>> [CONSULTA 5] Detección de Fricción: Carrito Abandonado")
session_friction = df.groupBy("user_session").agg(
    F.count(F.when(F.col("event_type") == "view", 1)).alias("total_vistas"),
    F.count(F.when(F.col("event_type") == "cart", 1)).alias("total_carritos"),
    F.count(F.when(F.col("event_type") == "purchase", 1)).alias("total_compras")
).filter((F.col("total_carritos") > 0) & (F.col("total_compras") == 0) & (F.col("total_vistas") >= 3)) \
 .orderBy(F.desc("total_carritos"))
session_friction.show(10, truncate=False)

# Creación de directorio gold
print("\n[INFO] Guardando vistas agregadas en la Zona Gold (/data/gold)...")
os.makedirs(GOLD_DATA_DIR, exist_ok=True)

funnel_metricas.write.mode("overwrite").parquet(os.path.join(GOLD_DATA_DIR, "gold_funnel_conversion"))
top_productos.write.mode("overwrite").parquet(os.path.join(GOLD_DATA_DIR, "gold_top_productos"))
rfm_df.write.mode("overwrite").parquet(os.path.join(GOLD_DATA_DIR, "gold_clientes_rfm"))
hourly_df.write.mode("overwrite").parquet(os.path.join(GOLD_DATA_DIR, "gold_trafico_horario"))
session_friction.write.mode("overwrite").parquet(os.path.join(GOLD_DATA_DIR, "gold_friccion_sesiones"))

print("[OK] Tablas analíticas almacenadas exitosamente en la capa Gold.")

# Exportación gráfica
print("\n[INFO] Generando visualización gráfica para evidencias...")

pdf_funnel = funnel_metricas.limit(7).toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
sns.set_theme(style="whitegrid")

# Gráfico 1: Facturación estimada por Marca Líder
pdf_top_marcas = (
    purchases_df.groupBy("brand")
    .agg(F.round(F.sum("price"), 2).alias("facturacion"))
    .filter(F.col("brand") != "Desconocido")
    .orderBy(F.desc("facturacion"))
    .limit(6)
    .toPandas()
)
sns.barplot(data=pdf_top_marcas, x="brand", y="facturacion", ax=ax1, palette="mako")
ax1.set_title("Top Marcas por Facturación Total", fontsize=12, fontweight="bold")
ax1.set_xlabel("Marca")
ax1.set_ylabel("Facturación Total ($)")

# Gráfico 2: Comparativa de Carritos vs Compras (Top Marcas)
pdf_melted = pdf_funnel.melt(
    id_vars=["brand"], 
    value_vars=["total_carritos", "total_compras"], 
    var_name="Tipo_Metrica", 
    value_name="Cantidad"
)
sns.barplot(data=pdf_melted, x="brand", y="Cantidad", hue="Tipo_Metrica", ax=ax2, palette="viridis")
ax2.set_title("Efectividad de Conversión: Carritos vs Compras", fontsize=12, fontweight="bold")
ax2.set_xlabel("Marca")
ax2.set_ylabel("Cantidad de Eventos")

plt.tight_layout()
output_chart = os.path.join(DATALAKE_DIR, "reporte_analitico_lakehouse.png")
plt.savefig(output_chart, dpi=300)
print(f"[OK] Gráfico mejorado guardado en: {output_chart}")

spark.stop()