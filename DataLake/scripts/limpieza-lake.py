import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Ruta de la carpeta donde esta los scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# La ruta del DataLake
DATALAKE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
# las rutas exactas definidas por el directorio del DataLake
RAW_DATA_DIR = os.path.join(DATALAKE_DIR, "data", "raw")
# Directorio para la zona preparada (silver)
PREPARED_DATA_DIR = os.path.join(DATALAKE_DIR, "data", "prepared")

def limpiar_datos_ecommerce(df):
    # Eliminar registros duplicados
    df_limpio = df.dropDuplicates()

    # Correcion tipos de datos
    df_limpio = df_limpio.withColumn("price", col("price").cast("float"))
    df_limpio = df_limpio.withColumn("event_time", to_timestamp(col("event_time")))

    # Eliminacion filas inservibles (sin usuario, producto o precio)
    df_limpio = df_limpio.dropna(subset=["product_id", "user_id", "price"])

    # Rellenar informacion faltante en texto como esta nulo
    df_limpio = df_limpio.fillna({"brand": "Desconocido", "category_code": "Sin categoria", "category_id": "0"})
    
    return df_limpio

if __name__ == "__main__":
    print("[INFO] Iniciando Apache Spark...")
    spark = SparkSession.builder.appName("LimpiezaZonaSilver").master("local[*]").getOrCreate()
    
    patron_busqueda = os.path.join(RAW_DATA_DIR, "*.csv")
    archivos_raw = glob.glob(patron_busqueda)
    
    if not archivos_raw:
        print(f"[ERROR] No se encontraron archivos CSV en la Zona Raw: {RAW_DATA_DIR}")
        print("Ejecuta primero el script de ingesta")
    else:
        # Selecciona el archivo mas reciente
        archivo_crudo = max(archivos_raw, key=os.path.getctime)
        print(f"[INFO] Leyendo datos desde: {archivo_crudo}")
        # Lee el archivo original
        df_crudo = spark.read.csv(archivo_crudo, header=True, inferSchema= True)

        print("[INFO] Aplicando transformaciones...")
        df_procesado = limpiar_datos_ecommerce(df_crudo)
        print("Esquema de datos corregido")
        df_procesado.printSchema()
        print(f"Total de registros tras la limpieza: {df_procesado.count()}")
        
        # Se crea el directorio de la zona preparada si no existe
        os.makedirs(PREPARED_DATA_DIR, exist_ok=True)

        # Genera el nombre de la carpeta y se envía a la zona preparada (en .parquet)
        nombre_archivo_base = os.path.basename(archivo_crudo).replace(".csv", ".parquet")
        ruta_salida_parquet = os.path.join(PREPARED_DATA_DIR, nombre_archivo_base)
        print(f"[INFO] Guardando datos en formato Parquet en: {ruta_salida_parquet}")

        # Escribe el DataFrame procesado en formato Parquet y usamos overwrite para reemplazar los datos si el archivo ya existe
        df_procesado.write.mode("overwrite").parquet(ruta_salida_parquet)
        print("[EXITO] Limpieza y Construcción de Zona Preparada finalizadas correctamente.")

        # Leer el Parquet generado
        print("Vista de parquet creado.")
        df_leido = spark.read.parquet(ruta_salida_parquet)

        # Mostrar las primeras 10 filas sin truncar el texto
        df_leido.show(10, truncate=False)
        # Ver el esquema generado
        df_leido.printSchema()
    
    # Liberar recursos del clúster local
    spark.stop()