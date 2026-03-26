
import os
import json
import datetime
import calendar
import time
import threading
from collections import defaultdict, Counter
from datetime import date

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pyspark.sql import SparkSession

_here = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.abspath(os.path.join(_here, "..", ".."))
load_dotenv(os.path.join(_repo_root, ".env"))
load_dotenv(os.path.join(_here, ".env"))

print("Initializing stream_processor (Structured Streaming)")

KAFKA_TOPIC_read = os.environ.get("KAFKA_TOPIC_READ", "wikipedia")
KAFKA_BROKER_read = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_BROKER_write = os.environ.get("KAFKA_BROKER_WRITE", KAFKA_BROKER_read)
KAFKA_TOPIC_write = os.environ.get("KAFKA_TOPIC_WRITE", "processed")
CHECKPOINT_DIR = os.environ.get("STREAM_CHECKPOINT_DIR", "checkpoint_wiki_structured")
TRIGGER_SECONDS = int(os.environ.get("STREAM_TRIGGER_SECONDS", "2"))
WINDOW_DURATION_SEC = int(os.environ.get("WINDOW_DURATION_SEC", "40"))
WINDOW_SLIDE_SEC = int(os.environ.get("WINDOW_SLIDE_SEC", "6"))


def _mongo_uri():
    """Same resolution as backend/flask/app.py: MONGO_URI or split env vars."""
    explicit = os.getenv("MONGO_URI")
    if explicit:
        return explicit
    user = os.getenv("MONGO_USERNAME") or os.getenv("USERNAME")
    password = os.getenv("MONGO_PASSWORD") or os.getenv("PASSWORD")
    host = os.getenv("MONGO_HOST") or os.getenv("HOST")
    database = os.getenv("MONGO_DATABASE") or os.getenv("DATABASE")
    return (
        "mongodb+srv://"
        + user
        + ":"
        + password
        + "@"
        + host
        + "/"
        + database
        + "?retryWrites=true&w=majority"
    )


MONGO_URI = _mongo_uri()

# Global line-chart series (same role as legacy driver-side lists)
additionLineChartData = []
additionLineChartLabels = []
deletionLineChartData = []
deletionLineChartLabels = []
noeditsLineChartData = []
noeditsLineChartLabels = []

# Running totals for aggregate path (replaces updateStateByKey)
aggregate_counts = defaultdict(int)

# Window buffer: (kafka_ts_ms, list of composite keys from extract_info_window)
window_events = []

# Hourly rolling counts for Mongo (replaces countByWindow(3600, 3600) semantics)
hourly_event_ts_ms = []

# Batch / timing coordination
_batch_index = [0]
_last_mongo_calendar_hour = [None]

_kafka_producer_lock = threading.Lock()
_kafka_producer = [None]


def _get_kafka_producer():
    with _kafka_producer_lock:
        if _kafka_producer[0] is None:
            brokers = [b.strip() for b in KAFKA_BROKER_write.split(",") if b.strip()]
            _kafka_producer[0] = KafkaProducer(bootstrap_servers=brokers)
        return _kafka_producer[0]


def usersListRestructure(raw_data):
    structured_list = []
    for ((out_key, in_key), value) in raw_data:
        structured_list.append([in_key, value])
    return structured_list


def dataLabelRestructure(raw_data):
    temp_dict = {}
    for ((out_key, in_key), value) in raw_data:
        temp_dict[in_key] = value
    structured_dict = {"labels": list(temp_dict.keys()), "data": list(temp_dict.values())}
    return structured_dict


def wikiCardsRestructure(raw_data, total_changes):
    structured_dict = {"data": []}
    for ((out_key, in_key), value) in raw_data:
        try:
            percent = int(round((100 * value) / total_changes))
        except Exception:
            percent = 50
        structured_dict["data"].append(
            {"domain": in_key, "count": value, "percent": percent}
        )
    return structured_dict


def trafficRestructure(data, labels, limit):
    if len(data) > limit:
        data.pop(0)
        labels.pop(0)
    return {"data": data, "labels": labels}


def extract_info_aggregate_json(json_str):
    """Same extraction intent as legacy extract_info_aggregate (Kafka value = JSON string)."""
    try:
        data = json.loads(json_str)
        return [
            (("Class", data["Class"]), 1),
            (("users" + data["Class"], data["User"]), 1),
        ]
    except Exception:
        return []


def extract_info_window_json(json_str):
    """Same extraction intent as legacy extract_info_window."""
    try:
        data = json.loads(json_str)
        if data["BOT"] == "False":
            return [
                (("Domain", data["Domain"]), 1),
                (("Bot", data["BOT"]), 1),
                (("User", data["User"]), 1),
                (("Class", data["Class"]), 1),
            ]
        return [
            (("Domain", data["Domain"]), 1),
            (("Bot", data["BOT"]), 1),
            (("Class", data["Class"]), 1),
        ]
    except Exception:
        return []


def _sort_aggregate_items(items):
    """Mirror legacy aggregate_ordered_Counts ordering (primary key group, secondary count desc)."""
    return sorted(items, key=lambda x: (x[0][0], -x[1]))


def _sort_window_items(items):
    """Mirror legacy window_ordered_Counts ordering."""
    return sorted(items, key=lambda x: (x[0][0], -x[1]))


def _get_class_count(sorted_pairs, class_name):
    for (k, v) in sorted_pairs:
        if k[0] == "Class" and k[1] == class_name:
            return v
    return 0


def _get_bot_count(sorted_pairs, bot_str):
    for (k, v) in sorted_pairs:
        if k[0] == "Bot" and k[1] == bot_str:
            return v
    return 0


def window_sendToKafka(sorted_window_pairs):
    """Build the same processed payload as legacy window_sendToKafka (driver-side)."""

    def doWrite():
        data_dict = defaultdict(dict)
        global additionLineChartData
        global additionLineChartLabels
        global deletionLineChartData
        global deletionLineChartLabels
        global noeditsLineChartData
        global noeditsLineChartLabels

        additionLineChart_data = _get_class_count(sorted_window_pairs, "additions")
        additionLineChart_label = (
            str(datetime.datetime.now().hour)
            + ":"
            + str(datetime.datetime.now().minute)
            + ":"
            + str(datetime.datetime.now().second)
        )
        additionLineChartData.append(additionLineChart_data)
        additionLineChartLabels.append(additionLineChart_label)
        data_dict["additionLineChart"] = trafficRestructure(
            additionLineChartData, additionLineChartLabels, 20
        )

        deletionLineChart_data = _get_class_count(sorted_window_pairs, "deletions")
        deletionLineChart_label = (
            str(datetime.datetime.now().hour)
            + ":"
            + str(datetime.datetime.now().minute)
            + ":"
            + str(datetime.datetime.now().second)
        )
        deletionLineChartData.append(deletionLineChart_data)
        deletionLineChartLabels.append(deletionLineChart_label)
        data_dict["deletionLineChart"] = trafficRestructure(
            deletionLineChartData, deletionLineChartLabels, 20
        )

        noeditsLineChart_data = _get_class_count(sorted_window_pairs, "noedits")
        noeditsLineChart_label = (
            str(datetime.datetime.now().hour)
            + ":"
            + str(datetime.datetime.now().minute)
            + ":"
            + str(datetime.datetime.now().second)
        )
        noeditsLineChartData.append(noeditsLineChart_data)
        noeditsLineChartLabels.append(noeditsLineChart_label)
        data_dict["noeditLineChart"] = trafficRestructure(
            noeditsLineChartData, noeditsLineChartLabels, 20
        )

        bot_count = _get_bot_count(sorted_window_pairs, "True")
        no_bot_count = _get_bot_count(sorted_window_pairs, "False")
        total_changes = bot_count + no_bot_count
        try:
            bot_percent = int(round((100 * bot_count) / total_changes))
        except Exception:
            bot_percent = 50
        data_dict["botPercent"] = {"percent": bot_percent}

        domainCountDonut_raw = [x for x in sorted_window_pairs if x[0][0] == "Domain"][
            :7
        ]
        data_dict["domainCountDonut"] = dataLabelRestructure(domainCountDonut_raw)

        userLeaderboard_raw = [x for x in sorted_window_pairs if x[0][0] == "User"][:10]
        data_dict["userLeaderboard"] = dataLabelRestructure(userLeaderboard_raw)

        topWikiCards_raw = [x for x in sorted_window_pairs if x[0][0] == "Domain"][:3]
        data_dict["topWikiCards"] = wikiCardsRestructure(topWikiCards_raw, total_changes)

        try:
            producer = _get_kafka_producer()
            producer.send(
                KAFKA_TOPIC_write,
                value=json.dumps(data_dict, ensure_ascii=False).encode("utf-8"),
            )
            producer.flush()
        except KafkaError:
            pass

    runner = threading.Thread(target=doWrite)
    runner.start()


def aggregate_sendToKafka(sorted_pairs):
    """Build the same keyChangesCard payload as legacy aggregate_sendToKafka."""

    def doWrite():
        data_dict = defaultdict(dict)
        usersadditionsList_raw = [x for x in sorted_pairs if x[0][0] == "usersadditions"][
            :10
        ]
        usersdeletionsList_raw = [x for x in sorted_pairs if x[0][0] == "usersdeletions"][
            :10
        ]
        usersnoeditsList_raw = [x for x in sorted_pairs if x[0][0] == "usersnoedits"][
            :10
        ]
        addDeleteNoedit_raw = [x for x in sorted_pairs if x[0][0] == "Class"]
        usersadditionsCount = sum(1 for x in sorted_pairs if x[0][0] == "usersadditions")
        usersdeletionsCount = sum(1 for x in sorted_pairs if x[0][0] == "usersdeletions")
        usersnoeditsCount = sum(1 for x in sorted_pairs if x[0][0] == "usersnoedits")

        for ((out_key, in_key), value) in addDeleteNoedit_raw:
            data_dict["keyChangesCard"][in_key] = value

        data_dict["keyChangesCard"]["usersadditionsList"] = usersListRestructure(
            usersadditionsList_raw
        )
        data_dict["keyChangesCard"]["usersdeletionsList"] = usersListRestructure(
            usersdeletionsList_raw
        )
        data_dict["keyChangesCard"]["usersnoeditsList"] = usersListRestructure(
            usersnoeditsList_raw
        )
        data_dict["keyChangesCard"]["usersadditionsCount"] = usersadditionsCount
        data_dict["keyChangesCard"]["usersdeletionsCount"] = usersdeletionsCount
        data_dict["keyChangesCard"]["usersnoeditsCount"] = usersnoeditsCount
        try:
            producer = _get_kafka_producer()
            producer.send(
                KAFKA_TOPIC_write,
                value=json.dumps(data_dict, ensure_ascii=False).encode("utf-8"),
            )
            producer.flush()
        except KafkaError:
            pass

    runner = threading.Thread(target=doWrite)
    runner.start()


def sendToMongo(value):
    client = MongoClient(MONGO_URI)
    db = client.get_database("wikiStats")
    daywise_changes = db.daywise_changes
    query = {"day": calendar.day_name[date.today().weekday()]}
    update = {"$set": {str(datetime.datetime.now().hour): value}}
    daywise_changes.update_one(query, update)
    print(
        "Update"
        + str(datetime.datetime.now().hour)
        + "to Mongo at"
        + calendar.day_name[date.today().weekday()]
    )
    client.close()


def _prune_hourly_buffer(now_ms):
    global hourly_event_ts_ms
    cutoff = now_ms - 3600 * 1000
    hourly_event_ts_ms = [t for t in hourly_event_ts_ms if t >= cutoff]


def _prune_window_buffer(now_ms):
    global window_events
    cutoff = now_ms - (WINDOW_DURATION_SEC * 1000)
    window_events = [e for e in window_events if e[0] >= cutoff]


def _window_counts_at(now_ms):
    """Sliding WINDOW_DURATION_SEC second window ending at now_ms (processing-time semantics)."""
    cutoff = now_ms - (WINDOW_DURATION_SEC * 1000)
    c = Counter()
    for ts_ms, keys in window_events:
        if ts_ms > cutoff and ts_ms <= now_ms:
            for k in keys:
                c[k] += 1
    return _sort_window_items(list(c.items()))


def process_batch(batch_df, _batch_id):
    """Single foreachBatch: one Kafka read fan-out (aggregate + window + hourly Mongo)."""
    global aggregate_counts
    global window_events
    global hourly_event_ts_ms

    if batch_df.isEmpty():
        return

    rows = batch_df.select("value", "timestamp").collect()
    now_ms = int(time.time() * 1000)

    for row in rows:
        json_str = row.value if isinstance(row.value, str) else str(row.value)
        ts = row.timestamp
        if ts is not None:
            kafka_ts_ms = int(ts.timestamp() * 1000)
        else:
            kafka_ts_ms = now_ms

        for pair in extract_info_aggregate_json(json_str):
            aggregate_counts[pair[0]] += pair[1]

        wkeys = [p[0] for p in extract_info_window_json(json_str)]
        if wkeys:
            window_events.append((kafka_ts_ms, wkeys))

        hourly_event_ts_ms.append(kafka_ts_ms)

    _prune_window_buffer(now_ms)
    _prune_hourly_buffer(now_ms)

    sorted_agg = _sort_aggregate_items(list(aggregate_counts.items()))
    aggregate_sendToKafka(sorted_agg)

    _batch_index[0] += 1
    slide_batches = max(1, WINDOW_SLIDE_SEC // TRIGGER_SECONDS)
    if _batch_index[0] % slide_batches == 0:
        sorted_window = _window_counts_at(now_ms)
        window_sendToKafka(sorted_window)

    nh = datetime.datetime.now().hour
    if _last_mongo_calendar_hour[0] is None:
        _last_mongo_calendar_hour[0] = nh
    elif nh != _last_mongo_calendar_hour[0]:
        sendToMongo(len(hourly_event_ts_ms))
        _last_mongo_calendar_hour[0] = nh


if __name__ == "__main__":
    print("Starting SparkSession for WikiStats")
    spark_packages = os.environ.get(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )
    spark = (
        SparkSession.builder.appName("WikiStats")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.jars.packages", spark_packages)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    starting_offsets = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER_read)
        .option("subscribe", KAFKA_TOPIC_read)
        .option("startingOffsets", starting_offsets)
        .load()
        .selectExpr("CAST(value AS STRING) as value", "timestamp")
    )

    query = (
        kafka_df.writeStream.foreachBatch(process_batch)
        .trigger(processingTime="%d seconds" % TRIGGER_SECONDS)
        .start()
    )

    print("Streaming started successfully (Structured Streaming)")
    try:
        query.awaitTermination()
    finally:
        query.stop()

