import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

# Ruta de scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Ruta de Lambda
LAMBDA_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
# Directorio del csv consolidado generado por streaming
RAW_DATA_DIR = os.path.join(LAMBDA_DIR, "data", "raw")
# Directorio de salida para la Capa Batch
PROCESSED_DATA_DIR = os.path.join(LAMBDA_DIR, "data", "processed")

def limpiar_datos_ecommerce(df):
    # Eliminar registros duplicados
    df_limpio = df.dropDuplicates()
    # Corrección de tipos de datos
    df_limpio = df_limpio.withColumn("price", col("price").cast("float"))
    df_limpio = df_limpio.withColumn("event_time", to_timestamp(col("event_time")))
    # Eliminación de filas inservibles (nulos)
    df_limpio = df_limpio.dropna(subset=["product_id", "user_id", "price"])
    # Rellenar metadatos faltantes
    df_limpio = df_limpio.fillna({"brand": "Desconocido", "category_code": "Sin categoria", "category_id": "0"})
    # Timestamp para saber si la Capa Batch está actualizada
    df_limpio = df_limpio.withColumn("fecha_procesamiento", current_timestamp())
    
    return df_limpio

if __name__ == "__main__":
    print("[INFO] Iniciando Apache Spark para Capa Batch (Lambda)...")
    spark = SparkSession.builder.appName("Procesamiento_Batch_Lambda").master("local[*]").getOrCreate()
    
    patron_busqueda = os.path.join(RAW_DATA_DIR, "*.csv")
    archivos_raw = glob.glob(patron_busqueda)
    
    if not archivos_raw:
        print(f"[ERROR] No se encontraron archivos CSV en la Zona Raw: {RAW_DATA_DIR}")
        print("Ejecuta primero el script de ingesta")
    else:
        # Selecciona el archivo consolidado más reciente
        archivo_crudo = max(archivos_raw, key=os.path.getctime)
        print(f"[INFO] Leyendo datos consolidados desde: {archivo_crudo}")
        # Lectura de esquema
        df_crudo = spark.read.csv(archivo_crudo, header=True, inferSchema=True)

        print("[INFO] Aplicando transformaciones...")
        df_procesado = limpiar_datos_ecommerce(df_crudo)
        print("Esquema de la Capa Batch:")
        df_procesado.printSchema()
        print(f"Total de registros listos para la Capa de Servicio: {df_procesado.count()}")
        
        # Se crea el directorio de la zona preparada si no existe
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        # Mantiene el nombre basado en el timestamp original del archivo raw
        nombre_archivo_base = os.path.basename(archivo_crudo).replace(".csv", ".parquet")
        ruta_salida_parquet = os.path.join(PROCESSED_DATA_DIR, nombre_archivo_base)
        print(f"[INFO] Guardando vistas Batch en Parquet en: {ruta_salida_parquet}")
        
        # Guardar usando Parquet y sobrescribe si ya existe
        df_procesado.write.mode("overwrite").parquet(ruta_salida_parquet)
        print("[ÉXITO] Procesamiento Batch finalizado. Capa lista para Serving.")
        
        # Leer el Parquet generado
        print("Vista de parquet creado.")
        df_leido = spark.read.parquet(ruta_salida_parquet)
        
        # Mostrar las primeras 10 filas sin truncar el texto
        df_leido.show(10, truncate=False)
        # Ver el esquema generado
        df_leido.printSchema()
    
    # Liberar recursos del clúster local
    spark.stop()