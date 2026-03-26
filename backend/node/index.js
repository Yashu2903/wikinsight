const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });
require('dotenv').config({ path: path.join(__dirname, '.env') });

const express = require('express');
const cors = require('cors');
const kafka = require('kafka-node');

const KAFKA_HOST = process.env.KAFKA_BROKER || process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';

const KafkaClient = kafka.KafkaClient;
const Consumer = kafka.Consumer;

const client = new KafkaClient({ kafkaHost: KAFKA_HOST });
client.on('error', function (err) {
  console.error('[kafka client]', err.message);
});

const topics = [{ topic: 'processed' }];
const options = {
  fetchMaxWaitMs: 10000,
  fetchMaxBytes: 1024 * 1024,
  encoding: 'utf8',
  requestTimeout: false,
};
const consumer = new Consumer(client, topics, options);
consumer.on('error', function (err) {
  console.error('[kafka consumer]', err.message);
  if (err.name === 'BrokerNotAvailableError' || /broker/i.test(String(err.message))) {
    console.error(
      'Kafka is not reachable at',
      KAFKA_HOST,
      '- start Zookeeper/Kafka, create topic "processed", and set KAFKA_BROKER in .env if not localhost.'
    );
  }
});

const app = express();
app.use(cors());

const sseClients = new Map();

function registerSseClient(path, req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });
  if (!sseClients.has(path)) {
    sseClients.set(path, new Set());
  }
  sseClients.get(path).add(res);
  req.on('close', () => {
    const set = sseClients.get(path);
    if (set) {
      set.delete(res);
      if (set.size === 0) {
        sseClients.delete(path);
      }
    }
  });
}

function broadcast(path, payload) {
  const set = sseClients.get(path);
  if (!set || set.size === 0) {
    return;
  }
  const line = 'data: ' + JSON.stringify(payload) + '\n\n';
  for (const clientRes of set) {
    try {
      clientRes.write(line);
    } catch (e) {
      /* ignore broken pipe */
    }
  }
}

consumer.on('message', function (message) {
  let data;
  try {
    data = JSON.parse(message.value.toString());
  } catch (e) {
    return;
  }

  if (data.domainCountDonut) {
    broadcast('/domainCountDonut', data.domainCountDonut);
  }
  if (data.keyChangesCard) {
    broadcast('/keyChangesCard', data.keyChangesCard);
  }
  if (data.botPercent) {
    broadcast('/botPercent', data.botPercent);
  }
  if (data.topWikiCards) {
    broadcast('/topWikiCards', data.topWikiCards);
  }
  if (data.userLeaderboard) {
    broadcast('/userLeaderboard', data.userLeaderboard);
  }
  if (data.additionLineChart || data.deletionLineChart || data.noeditLineChart) {
    broadcast('/lineChart', data);
  }
});

app.get('/', function (req, res) {
  res.send('hello world');
});

app.get('/domainCountDonut', function (req, res) {
  registerSseClient('/domainCountDonut', req, res);
});

app.get('/keyChangesCard', function (req, res) {
  registerSseClient('/keyChangesCard', req, res);
});

app.get('/botPercent', function (req, res) {
  registerSseClient('/botPercent', req, res);
});

app.get('/topWikiCards', function (req, res) {
  registerSseClient('/topWikiCards', req, res);
});

app.get('/userLeaderboard', function (req, res) {
  registerSseClient('/userLeaderboard', req, res);
});

app.get('/lineChart', function (req, res) {
  registerSseClient('/lineChart', req, res);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, function () {
  console.log('SSE API listening on port', PORT, '| Kafka:', KAFKA_HOST);
});
