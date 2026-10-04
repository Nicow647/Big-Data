import os
import pandas as pd
from datetime import datetime

# Ruta de la arquitectura Lambda
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Carpeta donde llegan los eventos
SPEED_DATA_DIR = os.path.join(BASE_DIR, "data", "speed")
# Zona Raw del DataLake
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")


def setup_directories(target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"[OK] Directorio creado: {target_dir}")
    else:
        print(f"[INFO] El directorio ya existe: {target_dir}")


def fetch_ecommerce_data(source_dir, output_path):
    print("[INFO] Leyendo eventos acumulados desde la Zona Speed...")
    # Buscar todos los CSV de eventos
    csv_files = [
        os.path.join(source_dir, file)
        for file in os.listdir(source_dir)
        if file.endswith(".csv")
    ]

    if not csv_files:
        print("[ERROR] No se encontraron archivos CSV en la Zona Speed.")
        return
    
    print(f"[INFO] Se encontraron {len(csv_files)} archivos de eventos.")
    # Leer y combinar los eventos
    dataframes = []
    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)
    
    df_final = pd.concat(dataframes, ignore_index=True)
    # Guardar en Raw
    df_final.to_csv(output_path, index=False)
    print(f"[OK] Se combinaron {len(df_final)} eventos " f"desde la Zona Speed.")
    print(f"[OK] Archivo guardado correctamente en: {output_path}")


if __name__ == "__main__":
    setup_directories(RAW_DATA_DIR)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_filename = f"raw_ecommerce_{timestamp}.csv"
    destination_path = os.path.join(RAW_DATA_DIR, raw_filename)

    fetch_ecommerce_data(SPEED_DATA_DIR, destination_path)
    print("[ÉXITO] Ingesta Batch completada desde la Zona Speed.")
