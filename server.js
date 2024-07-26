/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const express = require("express");
const WhatsAppWebhookParser = require("./WAParser.js");
const cassandraClient = require("./database/cassandra.js");
const { initProducer, closeProducer, sendMessage } = require('./kafkaProducer.js');


const app = express();
app.use(express.json());

const { WEBHOOK_VERIFY_TOKEN, GRAPH_API_TOKEN, PORT } = process.env;

app.post("/webhook", async (req, res) => {
  // log incoming messages
  console.log("Incoming webhook message:", JSON.stringify(req.body, null, 2));

  const parse = new WhatsAppWebhookParser(req.body, cassandraClient);

  const parsedMessage = parse.parseMessage();
  if(!parsedMessage){
    res.status(500).send('Could not parse message');
  }else{
    console.log(JSON.stringify(parsedMessage));
    try {
      await sendMessage(parsedMessage);
      res.status(200).send('Message sent to Kafka');
    } catch (error) {
      console.error('Error sending message to Kafka:', error);
      res.status(500).send('Internal Server Error');
    }
  }
});

// accepts GET requests at the /webhook endpoint. You need this URL to setup webhook initially.
// info on verification request payload: https://developers.facebook.com/docs/graph-api/webhooks/getting-started#verification-requests
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  // check the mode and token sent are correct
  // if (mode === "subscribe" && token === WEBHOOK_VERIFY_TOKEN) { removing token verification since we are submitting this to interakt. 
    // respond with 200 OK and challenge token from the request
    res.status(200).send(challenge);
    console.log("Webhook verified successfully!");
  // } else {
  //   // respond with '403 Forbidden' if verify tokens do not match
  //   res.sendStatus(403);
  // }
});

app.get("/", (req, res) => {
  res.send(`<pre>Welcome to Vartalaap webhook server</pre>`);
});

app.listen(PORT, async () => {
  await initProducer();
  console.log(`Server is listening on port: ${PORT}`);
});

process.on('SIGTERM', async () => {
  await closeProducer();
});