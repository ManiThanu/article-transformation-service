# BLOOMBERG MSYN ASSET SERVICE

As of now, this service is responsible for:

* All MySQL Reads & Writes (for Assets & Asset Mappings Data)
* Persisting Assembled Assets from Ingestion
* Notifying The Reverse Query Service to Update Asset Mappings
* Persisting Asset Mapping Updates from The Reverse Query Service
* Notifying The Push Delivery Service to Sync with Client FTP Servers
* Assembling Feeds/Subscriptions for The Pull Delivery Service
* Fulfilling Custom Queries for Portal

## Installation
### NVM
If you have NVM installed already please move to the next step.

For whatever reason NVM doesn't play nice with homebrew. If you found a work around feel free to update the README, otherwise please follow the instructons here: https://github.com/creationix/nvm

### Node.js
As of 10-25-2016, the engineers behind Node.js do not recommend using version 7 if you are developing for a medium to large enterprise and/or do not have the ability to make on-the-fly platform upgrades: https://nodejs.org/en/blog/release/v7.0.0/. As such, please make sure you have the latest **LTS** (long-term support) version of Node.js installed:

`$ nvm install 6.9.1`

###  MySQL
* `$ brew install mysql`
* `$ brew services start mysql`
* `$ mysql -u root`
* `mysql> CREATE DATABASE asset_service;`
* `mysql> CREATE DATABASE asset_service_testing;`

### Dependencies
`$ npm install`

## Usage
### Consumers
* Assets: `npm run consumer:asset`
* Mappings: `npm run consumer:mappings`

For your convenience, you can start & stop all services with a single command - respectively:

* Start Service Unit: `$ npm start`
* Stop Service Unit: `$ npm stop`

### Run DB Migrations
* Development: `$ NODE_ENV=development npm run db:migrate`
* Testing:     `$ NODE_ENV=testing npm run db:migrate`
* Integration: `$ NODE_ENV=integration npm run db:migrate`
* Staging:     `$ NODE_ENV=staging npm run db:migrate`
* Production:  `$ NODE_ENV=production npm run db:migrate`

### Run Test Suite
`$ npm test`

### Run Code Linter
`$ npm run lint`

## Contributing
Want to help?

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push upstream my-new-feature`
5. Submit a pull request

## Support
Find a bug? Please create a new issue in Github.
