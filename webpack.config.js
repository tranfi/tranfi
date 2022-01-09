const path = require('path')
const { VueLoaderPlugin } = require('vue-loader') // Support Vue's SFCs
const TerserPlugin = require('terser-webpack-plugin') // Minimize code
const webpack = require('webpack')
const package = require('./package.json')

module.exports = (env) => {
  const config = {
    entry: './src/main.js',
    output: {
      filename: 'bundle.js',
      path: path.resolve(__dirname, 'dist'),
    },
    module: {
      rules: [
        {
          test: /\.vue$/,
          loader: 'vue-loader'
        },
        {
          test: /\.js$/,
          loader: 'babel-loader'
        },
        {
          test: /\.mjs$/i, // lodash error (element)
          resolve: { byDependency: { esm: { fullySpecified: false } } }
        },
        {
          test: /\.html/,
          type: 'asset/source'
        },
        {
          test: /\.css$/,
          use: [
            'vue-style-loader',
            'css-loader',
          ]
        },
        {
          test: /worker\.js$/,
          loader: "worker-loader",
          options: {
            inline: 'no-fallback'
          },
        },
        {
          test: /\.scss$/,
          use: [
            'vue-style-loader',
            'css-loader',
            'sass-loader'
          ]
        }
      ]
    },
    plugins: [
      new VueLoaderPlugin(),
      // Replace those values in the code
      new webpack.DefinePlugin({
        'VERSION': JSON.stringify(package.version),
      }),
      // https://stackoverflow.com/a/68723223/2998960
      // Work around for Buffer is undefined:
      // https://github.com/webpack/changelog-v5/issues/10
      new webpack.ProvidePlugin({
          Buffer: ['buffer', 'Buffer'],
      }),
      new webpack.ProvidePlugin({
          process: 'process/browser',
      }),
    ],
    // Load different versions of vue based on RUNTIME value
    resolve: {
      fallback: {
        'buffer': require.resolve('buffer'),
        'stream': require.resolve('stream-browserify'),
        'url': require.resolve('url')
      },
      alias: {
        vue$: 'vue/dist/vue.runtime.esm-bundler.js'
      },
    },
    // Update recommended size limit
    performance: {
      hints: false,
      maxEntrypointSize: 512000,
      maxAssetSize: 512000
    },
    // Remove comments
    optimization: {
      minimize: true,
      minimizer: [new TerserPlugin({
        extractComments: false,
        terserOptions: {
          format: {
            comments: false,
          },
        }
      })],
    },
    // Source map
    devtool: env.DEVELOPMENT
      ? 'eval-source-map'
      : false
  }

  return config
}
