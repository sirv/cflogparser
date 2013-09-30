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
}

util.inherits(CFLogParser, events.EventEmitter);

exports.createParser = function (options) {
    return new CFLogParser(options);
};

CFLogParser.prototype.parse = function(){


    this.s3.listObjects({
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
                            cb(null, {
                                err: null,
                                obj: obj,
                                logs: null
                            });
                            return;
                        }

                        var logObject = cflogobject.createObject(this.config, this.s3, obj);

                        logObject
                            .on('error', function (err) {
                                cb(null, {
                                    err: err,
                                    obj: obj,
                                    logs: null
                                });
                            }.bind(this))
                            .on('data', function (logs) {

                                cb(null, {
                                    err: null,
                                    obj: obj,
                                    logs: logs
                                }); // send these logs array to main callback

                            }.bind(this));

                        _.defer(logObject.process.bind(logObject));

                    }.bind(this)
                }.bind(this)),
                function (err, data) {  // logs is array of array, need to flatten one level
                    this.emit('data', data);
                }.bind(this)
            )
        }
    }.bind(this));
}

CFLogParser.prototype.moveS3Object = function(obj, target){

    var source = this.config.s3.bucket + '/' + obj.Key;
    var targetKey = this.config.s3[target].prefix + path.basename(obj.Key);

    this.s3.copyObject({
        Bucket: this.config.s3[target].bucket,
        CopySource: source,
        Key: targetKey
    }, function(err, data){
        if(err){
            this.emit('error', '[' + obj.basename +']: ' + 'Failed to copy S3 object: ' + err, 'source=', source, 'target=', targetKey);
            return;
        }

        if(this.config.dryrun) return; // testing mode, don't delete files

        this.s3.deleteObject({
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