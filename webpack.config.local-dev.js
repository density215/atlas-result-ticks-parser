const webpack = require("webpack");
const fs = require("fs");
const path = require("path");
const nodeExternals = require("webpack-node-externals");

var dir_src = path.resolve(__dirname, "src");
var dir_build = path.resolve(__dirname, "build");

/*
 * These are the environment variables that can
 * be set when running webpack (for deployment)
 */

 // ENVIRONMENT
 // The mode webpack is running in (formerly NODE_ENV),
 // also used in the `metadata.distribution.environment` field
 var ENVIRONMENT = "development"

// API_SERVER
// the api server that is used to make all API calls to
// this var will be fed to the top react component.
var apiServer = process.env.API_SERVER || "atlas.ripe.net";

// PUBLIC_PATH
// this path should conform to the STATIC_BUILD_URL config setting
// in the atlas-ui django app.
// Also important for code splitting:
// all split files ('0.bundle.js') will be hosted prefixed with this
var publicPath = process.env.PUBLIC_PATH || "https://4041.ripe.net/";

// USE_ES, false will point to legacy API.
var useES = process.env.USE_ES && process.env.USE_ES === "true" ? true : false;

var legacyInfix =
  {
    "atlas.ripe.net": "/"
  }[apiServer] || "/deprecated-sql";

var EsInfix =
  {
    "atlas.ripe.net": "/experimental-es"
  }[apiServer] || "/";

// Package version comes from package.json
// note that there's also a PACKAGE_VERSION.txt
// but that's for reference on the production server.
// Server apps are built with @zeit/pkg so there completely
// self-contained and do not expose package.json
try {
  PACKAGE_VERSION = require("./package.json").version;
} catch (err) {
  throw "Cannot find either PACKAGE_VERSION.txt or package.json. Cannot continue";
}
console.log("version :\t" + PACKAGE_VERSION);

// BUILD is the build number from the jenkins job.
// Use the BUILD_NUMBER envvar that will be passed in
// by Jenkins to the Docker container.
// If webpack was called stand alone it will fill out
// the current datetime.
BUILD = process.env.BUILD_NUMBER || `${new Date().toLocaleString()}`;
console.log("build :\t" + BUILD);

console.log(path.resolve(__dirname));
console.log(`using api server ${apiServer}`);
console.log(`hosted from path ${publicPath}`);
console.log(`ElasticSearch ${useES}`);

// end environment variables

const config = {
  target: "node",
  mode: ENVIRONMENT,
  // with nodeExternals every dependency that needs to be compiled should
  // be white-listed here.
  externals: [nodeExternals({ whitelist: "@ripe-rnd/ui-datastores" })],
  entry: ["core-js", path.resolve(dir_src, "index.js")],
  output: {
    path: dir_build,
    filename: "server.js",
    libraryTarget: "commonjs2"
  },
  devtool: "inline-source-map",
  module: {
    rules: [
      {
        test: /\.js[x]?$/,
        use: {
          loader: "babel-loader"
        }
      },
      {
        test: /\.sql?$/,
        include: [/sql/],
        use: {
          loader: "raw-loader"
        }
      }
    ]
  },
  resolve: {
    extensions: ["*", ".js", ".jsx"],
    symlinks: false
  },
  plugins: [
    new webpack.DefinePlugin({
      __API_SERVER__: JSON.stringify(apiServer),
      __USE_ES__: JSON.stringify(useES),
      __LEGACY_INFIX__: JSON.stringify(legacyInfix),
      __ES_INFIX__: JSON.stringify(EsInfix),
      __PACKAGE_VERSION__: JSON.stringify(PACKAGE_VERSION),
      __BUILD__: JSON.stringify(BUILD),
      __ENVIRONMENT__: JSON.stringify(ENVIRONMENT)
    })
  ]
};

module.exports = config;
