"""payment-producer : publie des events de paiement fictifs dans Kafka."""

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.getenv("KAFKA_TOPIC", "payments")
INTERVAL = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "0.5"))
NB_CUSTOMERS = int(os.getenv("NB_CUSTOMERS", "50"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("payment-producer")

# Identifiant unique de cette instance + numéro de séquence :
# permet au ledger-service de détecter des messages perdus.
PRODUCER_ID = uuid.uuid4().hex[:8]

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": f"payment-producer-{PRODUCER_ID}",
        "acks": "all",                 # attendre l'ack de toutes les répliques ISR
        "enable.idempotence": True,    # pas de doublons en cas de retry
        "linger.ms": 5,
        "delivery.timeout.ms": 120000, # temps max pour livrer un message (retries inclus)
    }
)

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def on_delivery(err, msg):
    if err is not None:
        log.error("ECHEC livraison key=%s : %s", msg.key().decode(), err)
    else:
        log.info(
            "OK key=%s partition=%d offset=%d",
            msg.key().decode(), msg.partition(), msg.offset(),
        )


def random_payment(seq: int, customer_id: str | None = None) -> dict:
    # ~3% de gros montants pour déclencher le fraud-detector
    amount = random.uniform(1000, 5000) if random.random() < 0.03 else random.uniform(5, 300)
    return {
        "payment_id": str(uuid.uuid4()),
        "producer_id": PRODUCER_ID,
        "seq": seq,
        "customer_id": customer_id or f"C{random.randint(1, NB_CUSTOMERS):03d}",
        "amount": round(amount, 2),
        "currency": "EUR",
        "method": random.choice(["CARD", "PAYPAL", "TRANSFER"]),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def send(payment: dict):
    while True:
        try:
            producer.produce(
                TOPIC,
                key=payment["customer_id"],  # même client -> même partition -> ordre garanti
                value=json.dumps(payment),
                on_delivery=on_delivery,
            )
            return
        except BufferError:
            # file d'attente locale pleine (ex : cluster injoignable) -> on attend
            log.warning("Buffer local plein, attente...")
            producer.poll(1)


def main():
    log.info("Démarrage producer_id=%s bootstrap=%s topic=%s", PRODUCER_ID, BOOTSTRAP_SERVERS, TOPIC)
    seq = 0
    while running:
        # ~2% de rafales : 4 paiements du même client d'affilée (règle de vélocité)
        if random.random() < 0.02:
            customer = f"C{random.randint(1, NB_CUSTOMERS):03d}"
            batch = [random_payment(seq + i, customer) for i in range(4)]
        else:
            batch = [random_payment(seq)]

        for payment in batch:
            send(payment)
        seq += len(batch)

        producer.poll(0)  # déclenche les callbacks de livraison
        time.sleep(INTERVAL)

    log.info("Arrêt : flush des messages en attente...")
    remaining = producer.flush(30)
    log.info("Arrêté. Envoyés=%d, non livrés=%d", seq, remaining)


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        log.exception("Erreur Kafka fatale : %s", e)
        raise
