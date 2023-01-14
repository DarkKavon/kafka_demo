import json
import uuid
import random
from time import sleep
from kafka import KafkaProducer

NEW_ORDER_TOPIC = "new_order"

def serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers="localhost:29092", value_serializer=serializer)

products = [("burger", 12.5), ("fries", 4.5), ("beer", 6.7), ("cola", 3.5), ("salad", 9)]

for i in range(10):
    how_many_products = random.randint(1,7)
    order = []
    total = 0
    for j in range(how_many_products):
        current_product = random.choices(products)[0]
        total += current_product[1]
        order.append(current_product)
        
    data = {
        "id": str(uuid.uuid4()),
        "product_list": order,
        "total_cost": total
    }

    producer.send(NEW_ORDER_TOPIC, data)
    print(f"Order {data.get('id')} was sent.")

    sleep(random.randint(2,10))