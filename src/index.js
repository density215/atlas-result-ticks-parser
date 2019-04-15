import restify from "restify";
import * as handlers from "./requestHandlers";

var cluster = require("cluster");

if (cluster.isMaster) {
  console.log("Server is active. Forking workers now.");
  var cpuCount = require("os").cpus().length;
  for (var i = 0; i < cpuCount; i++) {
    cluster.fork();
  }
  cluster.on("exit", function(worker) {
    console.error("Worker %s has died! Creating a new one.", worker.id);
    cluster.fork();
  });
} else {
  const server = restify.createServer({
    name: "atlas-trends",
    version: __PACKAGE_VERSION__
  });

  const healthCheckResponse = (req, res, next) => {
    res.send(200, "");
    next();
  };

  // Ensure we don't drop data on uploads
  server.pre(restify.pre.pause());

  // Clean up sloppy paths like //todo//////1//
  server.pre(restify.pre.sanitizePath());

  // Handles annoying user agents (curl)
  server.pre(restify.pre.userAgentConnection());

  server.pre((req, res, next) => {
    // Accoess-Control-Allow-Origin should be set
    // by the reverse proxy in front of node (aka nginx)
    // setting this header twice will bounce in the browser.
    // Also mirroring the requesting origin is insecure.
    // res.header("Access-Control-Allow-Origin", req.header("origin"));
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
}
