/**
 * Consumer
 */

const consumer = require('minimist')(process.argv.slice(2)).type;

// Load Environment
require('./src/utilities/env.loader');

// Start Subscribers
require(`./${process.env.SRC_DIR_NAME}/consumers/${consumer}`); // eslint-disable-line

// Example Usage:
// $ NODE_ENV=development babel-node consumer.js --type 'asset'
