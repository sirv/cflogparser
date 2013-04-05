var path = require('path');
var zlib = require('zlib');
var fs = require('fs');
var csv = require('csv');
var _ = require('underscore');
var async = require('async');
var events = require("events");
var util = require("util");

// -------------------


exports.CFLogObject = CFLogObject = function (config, s3, obj) {
    this.config = config;
    this.s3 = s3;
    this.obj = obj;
    this.gunzip = zlib.createGunzip();

    this.gunzip.setEncoding = function(){}; // avoid csv.from.stream to setEncoding on gunzip stream which breaks it totally

    this.gunzip.on('error', function(err){
        this.emit('error', 'Failed to gunzip input data: ' + err);
        //this.gunzip.destroy();
    });

    this.downloaded = 0;

    this.obj.basename = path.basename(obj.Key);
}

util.inherits(CFLogObject, events.EventEmitter);

exports.createObject = function (config, s3, obj) {
    return new CFLogObject(config, s3, obj);
};

CFLogObject.prototype.download = function(){

    var limit = 64*1024; // download and un-gzip in 64kb portions

    var range = this.downloaded+limit-1;
    if(range >= this.obj.Size) range = this.obj.Size - 1;

    this.s3.client.getObject({
        Bucket: this.config.s3.bucket,
        Key: this.obj.Key,
        Range: 'bytes=' + this.downloaded + '-' + range
    }, function(err, data){
        if(err){
            this.emit('error', err);
            return;
        }

        if(this.gunzip.writable){

            this.gunzip.write(data.Body);

            if(this.downloaded + data.Body.length == this.obj.Size){
                this.gunzip.end();
                return;
            }

            this.downloaded += data.Body.length;

            this.download();
        }
    }.bind(this))
}

CFLogObject.prototype.process = function(){

    if(this.obj.basename.indexOf('.gz') == -1){ // not a gzipped log file?
        this.emit('error', 'Don\'t know what to do with this file: ' + this.obj.Key);
        return;
    }

    csv()
        .from.options({
            delimiter: "\t",
            columns: ['date', 'time',  'x-edge-location', 'sc-bytes', 'c-ip', 'cs-method', 'cs(Host)', 'cs-uri-stem', 'sc-status', 'cs(Referer)', 'cs(User-Agent)', 'cs-uri-query', 'cs(Cookie)', 'x-edge-result-type', 'x-edge-request-id']
        })
        .from.stream(this.gunzip)
        .to.array(function(logs){
            this.emit('data', logs);
        }.bind(this))
        .transform(function(row, index){
            if(row['time'] == null) return null; // these are first two lines in each log which are comments

            // decode some URI encoded fields
            _.each(['cs(User-Agent)', 'cs(Referer)', 'cs-uri-stem', 'cs-uri-query', 'cs(Cookie)'], function(field){
                row[field] = decodeURIComponent(row[field]);
            })

            // these are numbers
            _.each(['sc-status', 'sc-bytes'], function(field){
                row[field] = parseInt(row[field]);
            })

            try {
                // run through filters
                for (var i = 0; i < this.config.filters.length; i++) {
                    var stop = this.config.filters[i].apply(row);

                    if (stop === null) { // message dropped by filter
                        //console.log('message dropped by filter');
                        return;
                    } else if (stop === false) { // stop filter processing
                        //console.log('stop processing by filters');
                        break;
                    }
                }
            } catch(e){
                console.error(e);
            }

            return row;
        }.bind(this))
        .on('error', function(error){
            this.emit('error', 'CSV error: ' + error);
        });

    this.download();
}
