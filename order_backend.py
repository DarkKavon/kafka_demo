import json
from kafka import KafkaConsumer, KafkaProducer

NEW_ORDER_TOPIC = "new_order"
CONFIRMED_ORDER_TOPIC = "confirmed_order"

def serializer(data):
    return json.dumps(data).encode("utf-8")

consumer = KafkaConsumer(NEW_ORDER_TOPIC, bootstrap_servers="localhost:29092")
producer = KafkaProducer(bootstrap_servers="localhost:29092", value_serializer=serializer)

for mess in consumer:
    order = json.loads(mess.value.decode())
    id = order["id"]
    products = [i[0] for i in order["product_list"]]
    total = order["total_cost"]
    print(f"Order {id} is confirmed.\nProducts list: {' '.join(products)}\nTotal cost: {total}\n")

    data = {
        'product_list': products,
        'total': total
    }

    producer.send(CONFIRMED_ORDER_TOPIC, data)