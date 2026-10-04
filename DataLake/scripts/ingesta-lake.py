import os
import shutil
from datetime import datetime

# Ruta de la carpeta donde esta los scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# La ruta del DataLake
DATALAKE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
# las rutas exactas definidas por el directorio del DataLake
RAW_DATA_DIR = os.path.join(DATALAKE_DIR, "data", "raw")
source_file = os.path.join(DATALAKE_DIR, "ecommerce.csv")

def setup_directories(target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"[OK] Directorio creado: {target_dir}")
    else:
        print(f"[INFO] El directorio ya existe: {target_dir}")

def fetch_ecommerce_data(source_path, output_path):
    print("[INFO] Copiando datos originales a la Zona Raw...")
    # Archivo mantenido a su forma original
    shutil.copy2(source_path, output_path)
    print(f"[OK] Archivo guardado correctamente en: {output_path}")

if __name__ == "__main__":
    setup_directories(RAW_DATA_DIR)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_filename = f"raw_ecommerce_{timestamp}.csv"
    destination_path = os.path.join(RAW_DATA_DIR, raw_filename)
    
    fetch_ecommerce_data(source_file, destination_path)
    print("[ÉXITO] Ingesta completada en la Zona Raw.")