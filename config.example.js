module.exports = {
    aws: {
        "accessKeyId": "AAAAAAAAAAAAAAAAAAAAA",
        "secretAccessKey": "BBBBBBBBBBBBBBBBBBB",
        "region": "us-east-1"
    },
    s3: {
        bucket: "s3bucket", // S3 bucket where CloudFront stores its logs
        prefix: 'cf-logs/', // prefix (folder) where CloudFront stores its logs

        // where to put parsed logs (as backup)
        backup: {
            bucket: "s3bucket",
            prefix: 'cf-logs-parsed/'
        },

        // where to put logs which we failed to parse (shouldn't happen :) )
        failed: {
            bucket: "s3bucket",
            prefix: 'cf-logs-failed/'
        }
    },
    elasticsearch: {
        hostname: '127.0.0.1',
        index: {
            prefix: 'cflogs',
            settings : {
                number_of_shards : 5,
                number_of_replicas : 0
            }
        },
        options: {
            port: 9200,
            protocol: 'http',
            timeout: 60000
        }
    },
    maxFiles: 5, //maximum files to download and process at once
    delay: 10, // delay in seconds between parse runs
    dryrun: true, // DO NOT DELETE files from original bucket, used for testing

    /**
     *  each function defined in this array will be called for each log message received.
     *  'this' will be a message object
     *
     *  return null to drop the message
     *
     *  return false to stop further filtering
     *
     *  function should by synchronous at this moment
     */
    filters: [

        // example filter
        // parse some account-id and item-id and store them as additional fields
        function(){

            var matches = this['cs-uri-stem'].match(/^\/p\/(.+?)\/(.+?)\//); // cs-uri-stem=/p/5123/99/item

            if(matches){

                this['account-id'] = parseInt(matches[1]); // 5123
                this['item-id'] = parseInt(matches[2]); // 99
            }
        }

    ]

}
