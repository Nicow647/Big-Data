import os
import json
import pandas as pd
from kafka import KafkaConsumer

# Configuracion de rutas
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVING_DIR = os.path.join(SCRIPT_DIR, "data", "serving")
if not os.path.exists(SERVING_DIR):
    os.makedirs(SERVING_DIR)


def limpiar_diccionario(datos):
    # Validar los datos nulos, lo que hace es ve el tipo de dato y con el return rechaza el dato si falta informacion
    if (
        datos["id_producto"] is None
        or datos["id_usuario"] is None
        or datos["precio"] is None
    ):
        return None
    # Rellenar los datos vacios
    if datos["marca"] is None:
        datos["marca"] = "Desconocido"
    if datos["id_categoria"] is None:
        datos["id_categoria"] = "0"
    # Si pasa de los filtros anteriores se convierte a Pandas
    df = pd.DataFrame([datos])
    df["precio"] = df["precio"].astype(float)
    df["hora_evento"] = pd.to_datatime(df["hora_evento"])
    return df


if __name__ == "__main__":
    print("[INFO] Iniciando Kappa...")
    # Conexion a Kafka
    consumidor = KafkaConsumer(
        "eventos", bootstrap_servers=["localhost:9092"], auto_offset_reset="latest"
    )
    # Crear archivo Csv limpio
    if os.path.exits(OUTPUT_FILE):
        columnas = [
            "id_evento",
            "hora_evento",
            "tipo_evento",
            "id_producto",
            "id_categoria",
            "marca",
            "precio",
            "id_usuario",
            "id_sesion",
        ]
        pd.DataFrame(columns=columnas).to_csv(OUTPUT_FILE, index=False)
    # Bucle de recepcion de eventos
    for mensaje in consumidor:
        texto_crudo = mensaje.value.decode("utf-8")
        diccionario_evento = json.loads(texto_crudo)
        # Envia la funcion de limpieza
        df_final = limpiar_diccionario(diccionario_evento)
        # Guarda si el dato fue aceptado
        if df_final is not None:
            df_final.to.csv(OUTPUT_FILE, mode="a", header=False, index=False)
            print(
                f"[EXITO] Evento {diccionario_evento['id_evento']} guardado correctamente."
            )
        else:
            print(
                f"[DESCARTADO] Evento {diccionario_evento['id_evento']} ignorado por nulos."
            )
