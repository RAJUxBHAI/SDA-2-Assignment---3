"""
Member 4 - Inventory & Delivery Risk Monitoring
Retail FMCD Industry

Generates simulated warehouse stock movement events (stock in/out) and
delivery/logistics tracking events (dispatched, in transit, delivered,
delayed), and streams them into the Kafka topic 'inventory-logistics-data'.

Running stock levels are tracked per (warehouse, product) so that stock
genuinely depletes over time, giving the consumer real low-stock
conditions to detect.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "inventory-logistics-data"

PRODUCTS = [
    {"id": "P1001", "name": "55-inch Smart LED TV", "category": "Television"},
    {"id": "P1002", "name": "Double Door Refrigerator 260L", "category": "Refrigerator"},
    {"id": "P1003", "name": "1.5 Ton Split Inverter AC", "category": "Air Conditioner"},
    {"id": "P1004", "name": "Front Load Washing Machine 7Kg", "category": "Washing Machine"},
    {"id": "P1005", "name": "Microwave Oven 23L Convection", "category": "Kitchen Appliance"},
]

WAREHOUSES = ["WH-North", "WH-South", "WH-West", "WH-East"]
STORES = ["Store-Jaipur-01", "Store-Mumbai-04", "Store-Bangalore-02", "Store-Delhi-07"]

stock_levels = {}
for wh in WAREHOUSES:
    for p in PRODUCTS:
        stock_levels[(wh, p["id"])] = random.randint(15, 60)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def stock_movement_event():
    warehouse = random.choice(WAREHOUSES)
    product = random.choice(PRODUCTS)
    key = (warehouse, product["id"])

    event_type = random.choices(["stock_in", "stock_out"], weights=[35, 65], k=1)[0]

    if event_type == "stock_in":
        qty = random.randint(10, 40)
        stock_levels[key] += qty
    else:
        qty = random.randint(5, 20)
        stock_levels[key] = max(0, stock_levels[key] - qty)

    return {
        "event_type": event_type,
        "movement_id": str(uuid.uuid4()),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "warehouse": warehouse,
        "quantity": qty,
        "stock_level_after": stock_levels[key],
        "timestamp": now_iso(),
    }


def shipment_event():
    product = random.choice(PRODUCTS)
    warehouse = random.choice(WAREHOUSES)
    status = random.choices(
        ["dispatched", "in_transit", "delivered", "delayed"],
        weights=[30, 30, 30, 10],
        k=1,
    )[0]
    return {
        "event_type": status,
        "shipment_id": str(uuid.uuid4()),
        "product_id": product["id"],
        "product_name": product["name"],
        "warehouse": warehouse,
        "destination_store": random.choice(STORES),
        "quantity": random.randint(5, 30),
        "timestamp": now_iso(),
    }


def main():
    print(f"Starting Member 3 producer -> topic '{TOPIC}' (Ctrl+C to stop)\n")
    count = 0
    try:
        while True:
            record = stock_movement_event() if random.random() < 0.6 else shipment_event()
            producer.send(TOPIC, value=record)
            count += 1

            if "stock_level_after" in record:
                print(f"[{count}] {record['event_type']} | {record['warehouse']} | "
                      f"{record['product_name']} | stock_after={record['stock_level_after']}")
            else:
                print(f"[{count}] {record['event_type']} | {record['product_name']} | "
                      f"-> {record['destination_store']}")

            time.sleep(random.uniform(0.3, 1.0))
    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()