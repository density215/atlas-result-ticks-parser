module.exports = function(api) {
  var env = api.cache(() => process.env.NODE_ENV);

  return {
    env: {
      production: {
        plugins: [
          "@babel/transform-async-to-generator",
          "@babel/plugin-proposal-object-rest-spread",
          "@babel/plugin-proposal-class-properties"
        ]
      }
    },
    plugins: [
      "@babel/transform-async-to-generator",
      "@babel/plugin-proposal-object-rest-spread",
      "@babel/plugin-proposal-class-properties"
    ]
  };
};
