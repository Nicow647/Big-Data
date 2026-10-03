import sqlite3
import os
import pandas as pd

# Ruta raíz del proyecto
PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

# Ruta de la base de datos
DB_PATH = os.path.join(
    PROJECT_DIR,
    "Otras arquitecturas",
    "DataWarehouse",
    "data",
    "dw_staging.db"
)

def ejecutar_consultas():

    print("[INFO] Conectando al Data Warehouse...")

    conn = sqlite3.connect(DB_PATH)

    print("[OK] Conexión establecida.")

    # 1. Cantidad total de registros
    consulta_1 = """
        SELECT COUNT(*) AS total_registros
        FROM stg_ecommerce
    """

    resultado_1 = pd.read_sql_query(consulta_1, conn)
    print("\n--- TOTAL DE REGISTROS ---")
    print(resultado_1)

    # 2. Eventos por tipo
    consulta_2 = """
        SELECT event_type, COUNT(*) AS cantidad
        FROM stg_ecommerce
        GROUP BY event_type
        ORDER BY cantidad DESC
    """

    resultado_2 = pd.read_sql_query(consulta_2, conn)
    print("\n--- EVENTOS POR TIPO ---")
    print(resultado_2)

    # 3. Precio promedio por marca
    consulta_3 = """
        SELECT brand,
               COUNT(*) AS cantidad_eventos,
               ROUND(AVG(price), 2) AS precio_promedio
        FROM stg_ecommerce
        WHERE brand IS NOT NULL
          AND price IS NOT NULL
        GROUP BY brand
        ORDER BY cantidad_eventos DESC
        LIMIT 10
    """

    resultado_3 = pd.read_sql_query(consulta_3, conn)
    print("\n--- TOP 10 MARCAS POR CANTIDAD DE EVENTOS ---")
    print(resultado_3)

    conn.close()

    print("\n[ÉXITO] Consultas ejecutadas correctamente.")


if __name__ == "__main__":
    ejecutar_consultas()