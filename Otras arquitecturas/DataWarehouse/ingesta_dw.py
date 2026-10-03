# ==============================================================================
# SCRIPT DE INGESTA ETL (EXTRACCIÓN Y CARGA EN STAGING - DATA WAREHOUSE)
# Rol: Persona A (Ingesta de datos)
# Objetivo: Extraer el dataset CSV original de e-commerce y volcarlo directamente 
#           en una tabla temporal de Staging dentro de una Base de Datos (SQLite).
# ==============================================================================

import sqlite3
import pandas as pd
import os

# ------------------------------------------------------------------------------
# 1. RUTAS Y CONFIGURACIÓN DEL ENTORNO
# Definimos la carpeta exclusiva del Data Warehouse para mantener todo ordenado.
# ------------------------------------------------------------------------------
# Ruta raíz del proyecto Big-Data-main
PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

# Archivo CSV original
CSV_PATH = os.path.join(PROJECT_DIR, "DataLake", "ecommerce.csv")

# Carpeta del Data Warehouse
BASE_DIR = os.path.join(
    PROJECT_DIR, "Otras arquitecturas", "DataWarehouse"
)

# Carpeta donde se guardará la BD
DATA_DIR = os.path.join(BASE_DIR, "data")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
    print(f"[OK] Directorio creado para la BD: {DATA_DIR}")

# Base de datos SQLite
DB_PATH = os.path.join(DATA_DIR, "dw_staging.db")


# ------------------------------------------------------------------------------
# 2. PROCESO DE EXTRAER Y CARGAR (E - L)
# ------------------------------------------------------------------------------
def run_staging_ingestion():
    print("[INFO] Iniciando proceso ETL - Fase de Extracción y Carga en Staging...")
    
    try:
        # Step 1: Conexión a la base de datos relacional del Data Warehouse.
        # Si el archivo 'dw_staging.db' no existe, SQLite lo crea automáticamente.
        conn = sqlite3.connect(DB_PATH)
        print(f"[OK] Conexión establecida con la BD: {DB_PATH}")

        # Step 2: Extracción (E) - Leemos el archivo CSV de origen con Pandas
        print(f"[INFO] Leyendo archivo fuente desde: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)

        # Step 3: Carga en Staging (L) - Volcamos la información a la tabla 'stg_ecommerce'
        # Usamos if_exists="replace" para refrescar el área de Staging en cada corrida.
        df.to_sql("stg_ecommerce", conn, if_exists="replace", index=False)
        print("[OK] Datos cargados exitosamente en la tabla SQL 'stg_ecommerce'.")

        # Cierre de conexión seguro
        conn.close()
        print("[ÉXITO] Ingesta ETL finalizada correctamente. Conexión cerrada.")

    except FileNotFoundError:
        print(f"[ERROR] No se encontró el archivo base en: {CSV_PATH}")
        print("[PISTA] Verifica que 'ecommerce.csv' esté copiado en la carpeta 'DataWarehouse'.")
    except Exception as e:
        print(f"[ERROR] Ocurrió un fallo durante el proceso ETL: {e}")


# ==============================================================================
# BLOQUE DE EJECUCIÓN PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    run_staging_ingestion()