# payment-producer

Publie des events de paiement fictifs dans un topic Kafka (par défaut `payments`).

```json
{"payment_id":"…","producer_id":"a1b2c3d4","seq":42,"customer_id":"C017",
 "amount":129.9,"currency":"EUR","method":"CARD","ts":"2026-11-21T10:00:00+00:00"}
```

- **Clé = `customer_id`** : tous les paiements d'un client vont sur la même partition (ordre garanti par client).
- **`acks=all` + idempotence** : un message n'est confirmé que lorsqu'il est répliqué sur les ISR, sans doublon en cas de retry.
- `producer_id` + `seq` permettent au `ledger-service` de détecter des messages perdus.
- ~3 % de gros montants et ~2 % de rafales (4 paiements du même client) pour alimenter le `fraud-detector`.

Chaque envoi est loggé avec sa partition et son offset (`OK key=C017 partition=3 offset=1234`) ou son erreur (`ECHEC livraison ...`).

## Prérequis

- Le topic doit exister (l'app ne le crée pas).
- Le serveur doit pouvoir joindre les brokers via leurs `advertised.listeners`.

## Lancer

```bash
cp .env.example .env    # renseigner KAFKA_BOOTSTRAP_SERVERS
```

**Python (3.10+)**
```bash
python3 -m venv venv && . venv/bin/activate
pip install -r requirements.txt
python app.py
```

**Docker Compose**
```bash
docker compose up -d --build   # build + démarrage
docker compose logs -f         # suivre les logs
docker compose stop            # arrêt propre (flush / commit)
docker compose down            # arrêt + suppression du conteneur
```

## Configuration

| Variable | Défaut | Rôle |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | *(obligatoire)* | ex : `10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092` |
| `KAFKA_TOPIC` | `payments` | topic cible |
| `PRODUCE_INTERVAL_SECONDS` | `0.5` | délai entre deux envois |
| `NB_CUSTOMERS` | `50` | nombre de clients fictifs |

`Ctrl+C` / `docker stop` : flush des messages en attente avant l'arrêt.
