import os
import pandas as pd
from datetime import datetime

# Rutas dentro del directorio actual D:\juand\UNIVERSIDAD\BIGDATA
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# CORRECCIÓN: Se agrega "DataLake" a la ruta para que coincida con tu árbol de archivos
RAW_DATA_DIR = os.path.join(BASE_DIR, "DataLake", "data", "raw")

def setup_directories(target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"[OK] Directorio creado: {target_dir}")
    else:
        print(f"[INFO] El directorio ya existe: {target_dir}")

def fetch_ecommerce_data(source_path, output_path):
    print("[INFO] Cargando y volcando datos en la Zona Raw...")
    df = pd.read_csv(source_path)
    df.to_csv(output_path, index=False)
    print(f"[OK] Archivo guardado correctamente en: {output_path}")

if __name__ == "__main__":
    setup_directories(RAW_DATA_DIR)
    
    source_file = os.path.join(BASE_DIR, "DataLake", "ecommerce.csv")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_filename = f"raw_ecommerce_{timestamp}.csv"
    destination_path = os.path.join(RAW_DATA_DIR, raw_filename)
    
    fetch_ecommerce_data(source_file, destination_path)
    print("[ÉXITO] Ingesta completada en la Zona Raw.")
