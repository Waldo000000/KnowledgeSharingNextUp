'use strict';

console.log('Loading function');

const AWS = require('aws-sdk');
const https = require('https');
const url = require('url');

var doc = require('dynamodb-doc');
var docClient = new AWS.DynamoDB.DocumentClient();

const slack_url = 'https://hooks.slack.com/services/<incoming webhook URL>';
const slack_req_opts = url.parse(slack_url);
slack_req_opts.method = 'POST';
slack_req_opts.headers = {'Content-Type': 'application/json'};

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    //event.Records.forEach((record) => {
    //    console.log(record.eventID);
    //    console.log(record.eventName);
    //    console.log('DynamoDB Record: %j', record.dynamodb);
    //});
    
    // Query DynamoDB table and work out who's next up
    console.log("Scanning table.");
    var items = [];
    var params = {
        TableName: "SlackKnowledgeSharing",
        ProjectionExpression: "#usr, lastDelivered",
        ExpressionAttributeNames: {"#usr": "user"}
    };
    
    docClient.scan(params, onScan);
    
    function onScan(err, data) {
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            // print all the records
            console.log("Scan succeeded.");
            data.Items.forEach(function(record) {
               console.log(record.user + ": ", record.lastDelivered);
               items.push(record);
            });
    
            // continue scanning if we have more records
            if (typeof data.LastEvaluatedKey != "undefined") {
                console.log("Scanning for more...");
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                docClient.scan(params, onScan);
            }
            else {
                tellSlackWhoIsNextUp(items);
            }
        }
    }
    
    function tellSlackWhoIsNextUp() {
        console.log("Telling Slack who's next up.");
        var nextUp = items.sort(function(a, b) { return a.lastDelivered - b.lastDelivered; })[0].user;
        console.log("Next up: " + nextUp);
        
        var req = https.request(slack_req_opts, function (res) {
            if (res.statusCode === 200) {
              callback(null, 'posted to slack');
            } else {
              callback('status code: ' + res.statusCode);
            }
        });
        
        req.on('error', function(e) {
            callback('problem with request: ' + e.message);
        });
        
        req.write(JSON.stringify({
            text: 'Next up for knowledge sharing: <' + nextUp + '>'
        }));
        
        req.end();

    }
    callback(null, `Successfully processed ${event.Records.length} records.`);
};
