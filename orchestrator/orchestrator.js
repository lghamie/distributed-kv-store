var express = require('express');
var bodyParser = require('body-parser');
var ConsistentHashing = require('consistent-hashing');
var request = require('request');
var fs = require('fs');
var execSync = require('exec-sync');
var winston = require('winston');

var app = express();
app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded


var nodeStatus; //Estado del nodo 
var keepAliveTimeout = 200;

// Configuracion en codigo
var dataNodes = ['http://localhost:6750', 'http://localhost:6751', 'http://localhost:6752', 'http://localhost:6753'];
var orchestratorNodes = ['http://localhost:5750', 'http://localhost:5751', 'http://localhost:5752', 'http://localhost:5753'];
var decidingWeights = [];
// Leer parametros por consola
var port = process.argv[2] || 5750;
var leader = process.argv[3] == 'true' ? true : false;
var deleteConf = process.argv[4] == 'true' ? true : false;

var logger = new(winston.Logger)({
    'transports': [
        new(winston.transports.Console)(),
        new(winston.transports.File)({ filename: 'orchestrators.log' })
    ],
    'filters': [function(level, msg, meta) {
        return port + ": " + msg;
    }]

});

///////////// Iniciailizacion//////////////
var cons = new ConsistentHashing(dataNodes);

var dbPath = './orch-db/' + port;

if (deleteConf) {
    logger.info("Deleting conf");
    if (fs.existsSync(dbPath)) {
        execSync("rm -r " + dbPath);
    }
    fs.mkdirSync(dbPath);
}

var options = {
    dbPath: dbPath,
    standby: !leader
};

if (leader) {
    nodeStatus = 'master';
    startMaster(orchestratorNodes);
} else {
    nodeStatus = 'listener';
}

function startMaster(orchestratorNodes) {
    logger.info("Changed status to master");
    broadcastStatus();
}

function broadcastStatus() {
    setInterval(function() {
        for (var i = 0; i < orchestratorNodes.length; i++) {
            if (orchestratorNodes[i] != 'http://localhost:' + port)
                sendKeepAlive(orchestratorNodes[i]);
        }
    }, keepAliveTimeout);
}

function sendKeepAlive(node) {
    request(node + "/cluster/keepalive", function(err, res, body) {
        if (err) {
            //logger.info("Node " + node + " is down");
            return;
        }
        if (res.statusCode == 200) {
            var bod = JSON.parse(body);
            decidingWeights.push(+bod.port);
        }
        //logger.info("Node " + node + " is ok");
    });
}

var tot;

app.get('/cluster/keepalive', function(req, res) {
    clearTimeout(tot);

    tot = setTimeout(function() {
        logger.info("Master got offline");
        nodeStatus = 'deciding';
        decidingWeights = [];
        broadcastStatus();

        //Envia broadcast y espera respuesta de todos o un timeout.
        setTimeout(function() {
            var isNewMaster = true;
            for (var i = 0; i < decidingWeights.length; i++) {
                if (decidingWeights[i] < port) {
                    isNewMaster = false;
                    break;
                }
            }
            if (isNewMaster) {
                nodeStatus = 'master';
                startMaster();
            } else {
                nodeStatus = 'listening';
            }
        }, keepAliveTimeout + 100);

    }, keepAliveTimeout + 100);

    res.send({ status: "ok", port: port });

});

app.get('/get/:operation/:value', function(req, res) {
    var values = [];
    var finished = 0;
    var resp = {
        status: 'ok'
    };

    if (nodeStatus != 'master') {
        res.status(410);
        res.send({ 'status': 'err', 'err': 'Not current master' });
        return;
    }

    logger.info("Getting values " + req.params.operation + " than: " + req.params.value);
    //Al guardar los datos distribuidos con consistent-hashing tengo que consultar todos los nodos.
    for (var i in dataNodes) {
        request(dataNodes[i] + '/get/' + req.params.operation + '/' + req.params.value, function(err, res2, body) {
            finished++;
            if (err) {
                logger.error("Error retrieving from backend : " + err);
            }
            if (res2.statusCode == 200) {
                var partialRes = JSON.parse(body).results;
                values = values.concat(partialRes);
            }
            if (finished == dataNodes.length) {
                resp.results = values;
                res.send(resp);
            }
        });
    }
});

app.get('/get/:key', function(req, res) {

    if (!(req.params.key != null)) {
        res.send({ 'status': 'err', 'err': 'No key supplied' });
        return;
    }
    if (nodeStatus != 'master') {
        res.status(410);
        res.send({ 'status': 'err', 'err': 'Not current master' });
        return;
    }

    var node = cons.getNode(req.params.key);
    logger.info("Getting key " + req.params.key + " from node " + node);

    request(node + '/get/' + req.params.key).pipe(res);

});

app.post('/put', function(req, res) {

    if (!(req.body.key != null)) {
        res.send({ 'status': 'err', 'err': 'No key supplied' });
        return;
    }
    if (nodeStatus != 'master') {
        res.status(410);
        res.send({ 'status': 'err', 'err': 'Not current master' });
        return;
    }


    var node = cons.getNode(req.body.key);
    logger.info("Putting key " + req.body.key + " in node " + node);
    var reqOpts = {
        'method': 'POST',
        'json': true,
        'url': node + '/put',
        body: req.body
    };

    request(reqOpts).pipe(res);

    //   res.send(resp);
});


app.listen(port, function() {
    logger.info('Orchestrator listening on port ' + port);
});
