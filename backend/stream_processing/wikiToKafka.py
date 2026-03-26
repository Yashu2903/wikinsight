import os
from json import dumps, loads

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from sseclient import SSEClient

_here = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.abspath(os.path.join(_here, "..", ".."))
load_dotenv(os.path.join(_repo_root, ".env"))
load_dotenv(os.path.join(_here, ".env"))

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_WIKIPEDIA", "wikipedia")
DATA_SOURCE = os.environ.get(
    "WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)


def publishWikiChangeKafka():
    brokers = [b.strip() for b in KAFKA_BROKER.split(",") if b.strip()]
    producer = KafkaProducer(bootstrap_servers=brokers)
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "WikiStats-kafka-producer/1.0 (Python requests; local)",
    }
    with requests.get(
        DATA_SOURCE,
        stream=True,
        timeout=None,
        headers=headers,
    ) as resp:
        resp.raise_for_status()
        client = SSEClient(resp.iter_content(chunk_size=8192))
        for event in client.events():
            if event.event != "message":
                continue
            useful_info = {}
            try:
                change = loads(event.data)
            except ValueError:
                change = None
            if change is not None:
                try:
                    if change["type"] == "edit":
                        if change["length"]["old"] > change["length"]["new"]:
                            useful_info["Class"] = "deletions"
                        else:
                            useful_info["Class"] = "additions"
                    else:
                        useful_info["Class"] = "noedits"
                    useful_info["Type"] = change["type"]
                    useful_info["Domain"] = change["meta"]["domain"]
                    useful_info["Title"] = change["title"]
                    useful_info["BOT"] = str(change["bot"])
                    useful_info["User"] = change["user"]
                    useful_info["Timestamp"] = change["timestamp"]
                    useful_info["Comment"] = change["comment"]
                    useful_info["Topic"] = change["meta"]["topic"]
                    useful_info["Wiki"] = change["wiki"]
                except Exception:
                    pass

            try:
                producer.send(
                    KAFKA_TOPIC,
                    value=dumps(useful_info, ensure_ascii=False).encode("utf-8"),
                )
            except Exception:
                pass


if __name__ == "__main__":
    publishWikiChangeKafka()
