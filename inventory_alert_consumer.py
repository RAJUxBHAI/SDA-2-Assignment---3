"""
Member 4 - Inventory & Delivery Risk Monitoring
Retail FMCD Industry

Consumes warehouse stock movement and delivery/logistics events from the
Kafka topic 'inventory-logistics-data', writes every event live into
MongoDB, and raises an alert (also stored live in MongoDB) when:
  (a) stock for a product at a warehouse drops below LOW_STOCK_THRESHOLD, or
  (b) a shipment event is reported as 'delayed'.
"""

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer

from atlas_config import get_atlas_db, to_datetime

TOPIC = "inventory-logistics-data"
LOW_STOCK_THRESHOLD = 20

mongo_client, db = get_atlas_db()  # MongoDB Atlas (cloud), see atlas_config.py
events_collection = db["inventory_events"]
alerts_collection = db["inventory_alerts"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="inventory-alert-consumer-atlas",
)

already_alerted = set()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def raise_alert(alert_type, record, extra=None):
    alert = {
        "alert_type": alert_type,
        "product_name": record.get("product_name"),
        "warehouse": record.get("warehouse"),
        "detected_at": now_iso(),
        "detected_time": datetime.now(timezone.utc),
        "source_event": record,
    }
    if extra:
        alert.update(extra)
    alerts_collection.insert_one(alert)
    print(f"  !!! ALERT [{alert_type}] -> {record.get('product_name')} @ {record.get('warehouse')}")


def main():
    print(f"Listening on topic '{TOPIC}' and writing to MongoDB 'fmcd_streaming' database...\n")
    try:
        for message in consumer:
            record = message.value

            events_collection.insert_one({**record, "event_time": to_datetime(record["timestamp"])})
            print(f"[stored] {record['event_type']} | {record.get('product_name')}")

            if "stock_level_after" in record:
                key = (record["warehouse"], record["product_id"])
                if record["stock_level_after"] < LOW_STOCK_THRESHOLD:
                    if key not in already_alerted:
                        raise_alert(
                            "LOW_STOCK",
                            record,
                            {"stock_level": record["stock_level_after"], "threshold": LOW_STOCK_THRESHOLD},
                        )
                        already_alerted.add(key)
                else:
                    already_alerted.discard(key)

            if record["event_type"] == "delayed":
                raise_alert("DELIVERY_DELAY", record)

    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()