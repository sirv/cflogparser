var path = require('path');
var os = require("os");
var _ = require('underscore');

var elastical = require('elastical');

var cflogparser = require('./lib/cflogparser');

var configFile = path.resolve(__dirname, 'config.js');

var optimist = require('optimist')
    .usage('Usage: $0 [options]')
    .default('config', configFile);

var argv = optimist.argv;

if(argv.help || argv.h) {
    optimist.showHelp();
    return;
}

require("clim")(console, true);

var config = require(argv.config);

Application = function(){
    this.dynamicDelay = config.delay;

    this.indexName = null;

    this.messagesCache = [];

    this.elasticClient = new elastical.Client(config.elasticsearch.hostname, config.elasticsearch.options);

    this.rotateIndex();

    this.parser = cflogparser.createParser(config);

    this.parser.on('error', function(error){
        console.error('Error: ' + error);

    }).on('data', this.processLogs.bind(this));

    this.parser.parse();
}

Application.prototype.processLogs = function(logs){
    console.log('Parsed messages: ' + logs.length);

    var messages = [];

    this.rotateIndex();

    _.each(logs, function (msg) {

        msg.timestamp = new Date(msg['date'] + 'T' + msg['time']).getTime();
        msg.parsedTs = Date.now();

        messages.push({
            create: {
                index: this.indexName,
                type: 'log',
                data: msg
            }
        });

    }.bind(this))

    if(logs.length > 0){
        this.elasticClient.bulk(messages, function(err, res){
            if (err) {
                console.error('ElasticSearch.bulk() failed: ' + err);
            }
        }.bind(this));
    }

    if(logs.length == 0){
        if(this.dynamicDelay < 600){
            this.dynamicDelay *= 2;
            console.log('Increasing delay to ' + this.dynamicDelay + ' secs');
        }
    } else {
        this.dynamicDelay = config.delay;
    }

    setTimeout(_.bind(this.parser.parse, this.parser), 1000 * this.dynamicDelay);
}

Application.prototype.rotateIndex = function () {
    var d = new Date();
    var m = d.getMonth() + 1;

    var newIndexName = config.elasticsearch.index.prefix + '-' + d.getFullYear() + '-' + (m<10?('0'+m):m);

    this.elasticClient.indexExists(newIndexName, function(err, exists){
        if (err) {
            console.error('Failed to verify index "'+ newIndexName +'": ' + err);
            return;
        }
        if(!exists){
            this.elasticClient.createIndex(newIndexName, {
                'settings': config.elasticsearch.index.settings,
                'mappings': {
                    "log" : {

                    }
                }
            }, function(err, index, res){
                if (err) {
                    console.error('Failed to create index "'+ newIndexName +'": ' + err);
                } else {
                    console.log('Created new index: ' + newIndexName);
                    this.indexName = newIndexName;
                }
            }.bind(this));
        } else {
            this.indexName = newIndexName;
        }
    }.bind(this));
}

var app = new Application();
