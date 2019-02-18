const webpack = require("webpack");
const path = require("path");
const nodeExternals = require("webpack-node-externals");

var dir_src = path.resolve(__dirname, "src");
var dir_build = path.resolve(__dirname, "build");

const config = {
  target: "node",
  externals: [nodeExternals()],
  entry: [
    path.resolve(dir_src, "index.js")
  ],
  output: {
    path: dir_build,
    filename: "server.js",
    libraryTarget: "commonjs2"
  },
  devtool: 'inline-source-map',
  module: {
    rules: [
      {
        test: /\.js?$/,
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
  }
};

module.exports = config;