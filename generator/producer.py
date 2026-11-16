import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

CITIES = {
    "Casablanca": (33.5731, -7.5898, "CAS"),
    "Rabat": (34.0209, -6.8416, "RBA"),
    "Marrakech": (31.6295, -7.9811, "RAK"),
    "Tanger": (35.7595, -5.8340, "TNG"),
}
CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger", "healthy"]
STATUSES = ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"]


def ensure_restaurants(path="/workspace/data/restaurants.json"):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    restaurants = []
    city_names = list(CITIES.keys())
    for i in range(1, 201):
        city = city_names[i % len(city_names)]
        base_lat, base_lon, _ = CITIES[city]
        restaurants.append({
            "restaurant_id": f"R-{i:03d}",
            "name": f"FastFood {city} #{i}",
            "city": city,
            "category": random.choice(CATEGORIES),
            "latitude": round(base_lat + random.uniform(-0.04, 0.04), 6),
            "longitude": round(base_lon + random.uniform(-0.04, 0.04), 6),
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(restaurants, f, ensure_ascii=False, indent=2)
    return restaurants


def generate_event(seq_id, restaurants, base_time=None):
    rest = random.choice(restaurants)
    city = rest["city"]
    code = CITIES[city][2]
    now = base_time or datetime.now(timezone.utc)

    if random.random() < 0.05:
        delay_sec = random.randint(60, 900)
        event_dt = now - timedelta(seconds=delay_sec)
    else:
        event_dt = now

    status = random.choices(STATUSES, weights=[0.15, 0.15, 0.60, 0.10])[0]
    return {
        "order_id": f"ORD-{seq_id:07d}",
        "restaurant_id": rest["restaurant_id"],
        "customer_id": f"C-{random.randint(1, 5000):05d}",
        "city": city,
        "status": status,
        "amount_mad": round(random.uniform(35.0, 320.0), 2),
        "items_count": random.randint(1, 6),
        "distance_km": round(random.uniform(0.5, 12.0), 2),
        "courier_id": f"L-{code}-{random.randint(1, 30):02d}",
        "event_time": event_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file", "kafka"], required=True)
    parser.add_argument("--n", type=int, default=50000)
    parser.add_argument("--rate", type=int, default=20)
    parser.add_argument("--bootstrap", default="kafka:9092")
    args = parser.parse_args()

    restaurants = ensure_restaurants()
    recent_events = []

    if args.mode == "file":
        out_path = "/workspace/data/orders_history.jsonl"
        start_dt = datetime(2026, 11, 14, 8, 0, 0, tzinfo=timezone.utc)
        with open(out_path, "w", encoding="utf-8") as f:
            for i in range(1, args.n + 1):
                if recent_events and random.random() < 0.02:
                    ev = random.choice(recent_events)
                else:
                    sim_time = start_dt + timedelta(seconds=i * 0.5)
                    ev = generate_event(i, restaurants, sim_time)
                    recent_events.append(ev)
                    if len(recent_events) > 500:
                        recent_events.pop(0)
                f.write(json.dumps(ev) + "\n")
        print(f"Fichier généré : {out_path} ({args.n} lignes)")

    elif args.mode == "kafka":
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        seq = 100000
        interval = 1.0 / args.rate
        print(f"Publication Kafka sur 'orders' à {args.rate} msg/s (Ctrl+C pour arrêter)...")
        try:
            while True:
                t0 = time.time()
                if recent_events and random.random() < 0.02:
                    ev = random.choice(recent_events)
                else:
                    seq += 1
                    ev = generate_event(seq, restaurants)
                    recent_events.append(ev)
                    if len(recent_events) > 500:
                        recent_events.pop(0)

                producer.send("orders", key=ev["city"], value=ev)
                elapsed = time.time() - t0
                if elapsed < interval:
                    time.sleep(interval - elapsed)
        except KeyboardInterrupt:
            producer.flush()
            print("Producteur arrêté proprement.")


if __name__ == "__main__":
    main()