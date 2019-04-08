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

  let PACKAGE_VERSION, BUILD;
  try {
    PACKAGE_VERSION = fs.readFileSync("./PACKAGE_VERSION.txt", "utf8");
  } catch (err) {
    if (err.code === "ENOENT") {
      PACKAGE_VERSION = JSON.stringify(require("./package.json").version);
    } else {
      throw "Cannot find either PACKAGE_VERSION.txt or package.json. Cannot continue";
    }
  }
  console.log("version :\t" + PACKAGE_VERSION);
  
  try {
    BUILD = fs.readFileSync("./BUILD.txt", "utf8");
  } catch (err) {
    if (err.code === "ENOENT") {
      BUILD = "not-built-dev";
    } else {
      throw "Something wrong with BUILD information. Cannot continue";
    }
  }
  console.log("build :\t" + BUILD);

console.log(path.resolve(__dirname));
console.log(`using api server ${apiServer}`);
console.log(`hosted from path ${publicPath}`);
console.log(`ElasticSearch ${useES}`);

// end environment variables

const config = {
  target: "node",
  mode: "production",
  // with nodeExternals every dependency that needs to be compiled should
  // be white-listed here.
  // externals: [nodeExternals({ whitelist: "@ripe-rnd/ui-datastores" })],
  externals: [nodeExternals({ whitelist: "rtthmm" })],
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
      __PACKAGE_VERSION__: PACKAGE_VERSION,
      __BUILD__: BUILD
    })
  ]
};

module.exports = config;
