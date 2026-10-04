import os, time, json
import pandas as pd
from kafka import KafkaProducer

BASE_DIR = r"../"


proc = KafkaProducer(
	bootstrap_servers=['localhost:9092'],
	value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv(os.path.join("ecommerce.csv"))

contador = 0

for index, row in df.iterrows():
	eve = {
		"id_evento": contador,
		"hora_evento": row["event_time"],
		"tipo_evento": row["event_type"],
		"id_producto": row["product_id"],
		"id_categoria":row["category_id"],
		"marca": row["brand"],
		"precio": row["price"],
		"id_usuario": row["user_id"],
		"id_sesion": row["user_session"]
	}
	# print(eve)
	proc.send("eventos",key=row["event_time"], value=eve)
	time.sleep(1)
	contador += 1

proc.close()
