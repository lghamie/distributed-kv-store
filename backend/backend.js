var express = require('express');
var levelup = require('level');
var bodyParser = require('body-parser');
var app = express();
app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded
var winston = require('winston');


var port = process.argv[2] || 6750;
var maxKeys = process.argv[3] || 100;

var insertedKeys = 0;
var db = levelup('./storage-' + port);


var logger = new(winston.Logger)({
    'transports': [
        new(winston.transports.Console)(),
        new(winston.transports.File)({ filename: 'backends.log' })
    ],
    'filters': [function(level, msg, meta) { return  port + ": " + msg;}]

});

app.get('/get/:key', function(req, res) {
    var resp = {
        status: 'ok'
    };

    if (!(req.params.key != null)) {
        resp.status = 'err';
        resp.err = 'no key specified';
        res.send(resp);
        return;
    }

    // Fetch by key
    db.get(req.params.key, function(err, value) {

        if (err) {
            resp.status = 'err';
            resp.err = "" + err;
            res.send(resp);
            return logger.info('Error getting key ' + req.params.key + ": " + err); // likely the key was not found
        }

        resp.value = value;
        logger.info('Retrieving key  ' + req.params.key);
        res.send(resp);

    });


});


app.get('/get/:operation/:value', function(req, res) {
    var resp = {
        status: 'ok'
    };

    if (!(req.params.value != null)) {
        resp.status = 'err';
        resp.err = 'No value specified';
        res.send(resp);
        return;
    }


    if (!(req.params.operation != null)) {
        resp.status = 'err';
        resp.err = 'No operation specified';
        res.send(resp);
        return;
    }
    var options = {};
    options[req.params.operation] = req.params.value;
    var results = [];
    logger.info("Getting values ", options);
    db.createReadStream(options)
        .on('data', function(data) {
        //    logger.info(data.key, '=', data.value);
            results.push(data.value);
        })
        .on('error', function(err) {
            logger.info('Error getting values: ', err);
            var resp = {
                'status': 'err',
                'err': "" + err
            };
            resp.results = results;
            res.send(resp);
        })
        .on('close', function() {
            var resp = { 'status': 'ok' };
            resp.results = results;
            logger.info('Got ' + resp.results.length + ' values ');
            res.send(resp);
        })
        .on('end', function() {
            //logger.info('Stream ended');
        });


});

app.post('/put', function(req, res) {
    var resp = {
        status: 'ok'
    };
    // Put a key & value
    db.put(req.body.key, req.body.value, function(err) {
        if (err) {
            resp.status = 'err';
            resp.err = "" + err;
            res.send(resp);
            return logger.info('Error putting in ' + req.body.key + ' value ' + req.body.value + "  err: " + err); // likely the key was not found
        }
        logger.info('Adding key  ' + req.body.key);
        insertedKeys++;
        res.send(resp);
    });


});


app.listen(port, function() {
    logger.info('Data node listening on port ' + port);
});
