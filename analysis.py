import json
from kafka import KafkaConsumer

CONFIRMED_ORDER_TOPIC = "confirmed_order"

consumer = KafkaConsumer(CONFIRMED_ORDER_TOPIC, bootstrap_servers="localhost:29092")

orders_freq = dict()
total_revenue = 0

for mess in consumer:
    order = json.loads(mess.value.decode())
    products = order["product_list"]
    total = order["total"]

    total_revenue += total
    for p in products:
        if p in orders_freq.keys():
            orders_freq[p] += 1
        else:
            orders_freq[p] = 1
    
    print(f"Current revenue: {round(total_revenue, 2)}\nCurrent order count: {orders_freq}\n")
