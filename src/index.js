import restify from "restify";
//import { plugins } from "restify";
import plugins from "restify-plugins";

import * as handlers from "./requestHandlers";

const server = restify.createServer({
  name: "atlas-trends",
  version: "0.0.1"
});

const healthCheckResponse = (req, res, next) => { res.send(200, ""); next() };

// Ensure we don't drop data on uploads
server.pre(restify.pre.pause());

// Clean up sloppy paths like //todo//////1//
server.pre(restify.pre.sanitizePath());

// Handles annoying user agents (curl)
server.pre(restify.pre.userAgentConnection());

server.pre((req, res, next) => {
  res.header("Access-Control-Allow-Origin", req.header("origin"));
  res.header(
    "Access-Control-Allow-Headers",
    req.header("Access-Control-Request-Headers")
  );
  res.header("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") {
    return res.send(204);
  }

  next();
});

server.use(restify.plugins.acceptParser(server.acceptable));
server.use(restify.plugins.fullResponse());
server.use(restify.plugins.queryParser());
server.use(restify.plugins.gzipResponse());
server.use(restify.plugins.bodyParser());

server.get(
  "/trends/:msmId/:prbId/summary",
  handlers.msmTrendsForProbe({ type: "summary" })
);
server.get(
  "/trends/:msmId/:prbId",
  handlers.msmTrendsForProbe({ type: "raw" })
);
server.get("/ticks/:msmId/:prbId", handlers.msmTicksForProbe);

server.get("/check", healthCheckResponse);

server.listen(process.env.LISTEN_PORT, () => {
  console.log("%s listening at %s", server.name, server.url);
});
