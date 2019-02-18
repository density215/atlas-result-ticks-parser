import restify from "restify";
import plugins from "restify-plugins";

import * as handlers from "./requestHandlers";

const server = restify.createServer({
  name: "atlas-trends",
  version: "0.0.1"
});

// Ensure we don't drop data on uploads
server.pre(restify.pre.pause());

// Clean up sloppy paths like //todo//////1//
server.pre(restify.pre.sanitizePath());

// Handles annoying user agents (curl)
server.pre(restify.pre.userAgentConnection());

server.use(restify.plugins.acceptParser(server.acceptable));
// server.use(restify.plugins.CORS());
server.use(restify.plugins.fullResponse());
server.use(restify.plugins.queryParser());
server.use(restify.plugins.gzipResponse());
server.use(restify.plugins.bodyParser());

server.get("/trends/:msmId", handlers.msmTrends);

server.listen(process.env.LISTEN_PORT, () => {
  console.log("%s listening at %s", server.name, server.url);
});
