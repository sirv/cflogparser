var path = require('path');
var AWS = require('aws-sdk');
var _ = require('underscore');
var async = require('async');
var events = require("events");
var util = require("util");

var cflogobject = require('./cflogobject');

// -------------------


exports.CFLogParser = CFLogParser = function (config) {
    this.config = config;

    AWS.config.update(this.config.aws);

    this.s3 = new AWS.S3();

    /*
    var shutdownFunc = function () {
        this.saveMarker();
    }.bind(this);

    process.on('SIGINT', shutdownFunc);
    process.on('SIGTERM', shutdownFunc);
    */
}

util.inherits(CFLogParser, events.EventEmitter);

exports.createParser = function (options) {
    return new CFLogParser(options);
};

CFLogParser.prototype.parse = function(){


    this.s3.client.listObjects({
        Bucket: this.config.s3.bucket,
        Delimiter: '/',
        Prefix: this.config.s3.prefix,
        MaxKeys: this.config.maxFiles || 5
    }, function(err, data){
        //console.log(data);
        if(err){
            this.emit('error', err)
            return;
        }

        if(!data || !data.Contents || data.Contents.length == 0){
            this.emit('data', []);
        } else {

            async.parallel(
                _.map(data.Contents, function (obj, index) {
                    return function (cb) {

                        if(obj.Size == 0){
                            cb(null, []);
                            return;
                        }

                        var logObject = cflogobject.createObject(this.config, this.s3, obj);

                        logObject
                            .on('error', function (err) {
                                this.emit('error', '[' + obj.basename +']: ' + err);
                                cb(null, []);
                                logObject.cleanup();
                                this.failed(obj);
                            }.bind(this))
                            .on('data', function (logs) {
                                console.log('[' + obj.basename +'] messages: ' +  logs.length);
                                cb(null, logs); // send these logs array to main callback
                                logObject.cleanup();
                                this.backup(obj);
                            }.bind(this));

                        _.defer(logObject.process.bind(logObject));

                    }.bind(this)
                }.bind(this)),
                function (err, logs) {  // logs is array of array, need to flatten one level
                    this.emit('data', _.flatten(logs, true));
                }.bind(this)
            )
        }
    }.bind(this));
}

// private

CFLogParser.prototype.moveS3Object = function(obj, target){
    this.s3.client.copyObject({
        Bucket: this.config.s3[target].bucket,
        CopySource: this.config.s3.bucket + '/' + obj.Key,
        Key: this.config.s3[target].prefix + path.basename(obj.Key)
    }, function(err, data){
        if(err){
            this.emit('error', '[' + obj.basename +']: ' + 'Failed to copy S3 object: ' + err);
            return;
        }

        if(this.config.dryrun) return; // testing mode, don't delete files

        this.s3.client.deleteObject({
            Bucket: this.config.s3.bucket,
            Key: obj.Key
        }, function(err, data){
            if(err){
                this.emit('error', '[' + obj.basename +']: ' + 'Failed to delete S3 object: ' + err);
                return;
            }
        }.bind(this))

    }.bind(this))
}

CFLogParser.prototype.backup = function(obj){
    this.moveS3Object(obj, 'backup');
}

CFLogParser.prototype.failed = function(obj){
    this.moveS3Object(obj, 'failed');
}
