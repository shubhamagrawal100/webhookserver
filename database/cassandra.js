const cassandra = require("cassandra-driver");
const mode = process.env.NODE_ENV || "production";
const config = require("../config/config")[mode.trim()];

const cloud = {
  secureConnectBundle: "./database/secure-connect-vartalaap.zip",
};
const authProvider = new cassandra.auth.PlainTextAuthProvider(
  "token",
  config.cassandra.token
);
const cassandraClient = new cassandra.Client({
  cloud,
  authProvider,
  keyspace: config.cassandra.keyspace,
});

cassandraClient
  .connect()
  .then(() => {
    console.log("Cassandra Connected");
  })
  .catch((err) => {
    console.log("Cassandra Connection Error", err);
  });

module.exports = cassandraClient;
