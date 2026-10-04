import os
import pandas as pd


# Ruta base de la arquitectura Lambda
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Archivo de entrada
INPUT_FILE = os.path.join(BASE_DIR, "ecommerce.csv")

# Carpeta donde se guardarán los datos limpios
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "clean")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def limpiar_datos(df):
    print("[INFO] Iniciando limpieza de datos...")

    # Eliminar registros duplicados
    df = df.drop_duplicates()

    # Eliminar registros sin precio
    df = df.dropna(subset=["price"])

    # Eliminar registros sin usuario
    df = df.dropna(subset=["user_id"])

    # Rellenar otros valores nulos
    df = df.fillna("desconocido")

    # Convertir precio a número decimal
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Eliminar nuevamente registros cuyo precio no pudo convertirse
    df = df.dropna(subset=["price"])

    return df


def main():
    print("[INFO] Leyendo datos de Lambda...")

    df = pd.read_csv(INPUT_FILE)

    registros_iniciales = len(df)

    df_limpio = limpiar_datos(df)

    registros_finales = len(df_limpio)

    output_file = os.path.join(OUTPUT_DIR, "ecommerce_clean.csv")

    df_limpio.to_csv(output_file, index=False)

    print(f"[OK] Registros iniciales: {registros_iniciales}")
    print(f"[OK] Registros después de la limpieza: {registros_finales}")
    print(f"[OK] Archivo limpio guardado en: {output_file}")
    print("[ÉXITO] Limpieza finalizada correctamente.")


if __name__ == "__main__":
    main()