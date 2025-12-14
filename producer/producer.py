from kafka import KafkaProducer
import time, random, os

KBOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=KBOOT,
    value_serializer=lambda v: v.encode()
)

MIN_N = 15_000
MAX_N = 16_000

while True:
    n = random.randint(MIN_N, MAX_N)
    ts = time.time()  # segundos epoch
    msg = f"{n}|{ts}"
    producer.send("input", msg)
    print("input", msg)
    producer.flush()
    time.sleep(0.0001)
