import os
import pandas as pd
import time
from datetime import datetime


def setup_directories(target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"[OK] Directorio creado: {target_dir}")
    else:
        print(f"[INFO] El directorio ya existe: {target_dir}")


def stream_ecommerce_data(source_path, output_dir):
    print("[INFO] Cargando datos origen para simulación Streaming...")
    try:
        # Leemos el archivo completo
        df = pd.read_csv(source_path)
        print("[INFO] iniciando emisor de eventos. Presiona Ctrl+C para detener.")

        # Iteramos fila por fila simulando la llegada en tiempo real
        for index, row in df.iterrows():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Creamos un archivo individual para cada evento (compra)
            event_filename = f"evento_{timestamp}_{index+1}.csv"
            event_path = os.path.join(output_dir, event_filename)

            # Convertimos la fila actual de nuevo a DataFrame para guardarla con su encabezado
            df_evento = pd.DataFrame([row])
            df_evento.to_csv(event_path, index=False)

            print(f"[STREAM] Evento {index+1} simulado y guardado en: {event_filename}")

            # Pausa de 1 segundo entre fila y fila
            time.sleep(1)

    except FileNotFoundError:
        print(f"[ERROR] No se encontró el archivo base en: {source_path}")
    except KeyboardInterrupt:
        print("\n[INFO] Simulación de streaming parada por el usuario.")


if __name__ == "__main__":

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    SPEED_DATA_DIR = os.path.join(BASE_DIR, "data", "speed")

    setup_directories(SPEED_DATA_DIR)

    source_file = os.path.join(BASE_DIR, "ecommerce.csv")

    stream_ecommerce_data(source_file, SPEED_DATA_DIR)
