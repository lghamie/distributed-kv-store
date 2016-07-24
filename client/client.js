var rp = require('request-promise');
var randomstring = require("randomstring");
var winston = require('winston');

var logger = new(winston.Logger)({
    'transports': [
        new(winston.transports.Console)(),
        new(winston.transports.File)({ filename: 'backends.log' })
    ],
    'filters': [function(level, msg, meta) {
        return msg;
    }]

});

var maxKeySize = 8;
var maxValueSize = 256;
var currentMaster = 0;

var orchestrators = [
    'http://localhost:5750',
    'http://localhost:5751',
    'http://localhost:5752',
    'http://localhost:5753',
];

var randomValues = {};
var notSentValues = [];
var alreadySentValues = [];

//Genera 10K de k-v random
for (var i = 0; i < 10000; i++) {
    var key = randomstring.generate(5);
    var value = randomstring.generate();

    randomValues[key] = value;
    notSentValues.push({ key: key, value: value });
}

//Every sec puts a value
setInterval(function() {
    var valueToSend = notSentValues.pop();
    var options = {
        method: 'POST',
        uri: orchestrators[currentMaster] + '/put',
        body: {
            'key': valueToSend.key,
            'value': valueToSend.value
        },
        json: true
    };
    rp(options)
        .then(function(res) {
            alreadySentValues.push(valueToSend);
            logger.info('Put' , res);
        })
        .catch(function(err) {
            logger.warn("Error calling orchestrator " + orchestrators[currentMaster] + ": " + err);
            getNextMaster();
        });

}, 100);

function getNextMaster(){
    var newMaster;
    if (currentMaster == orchestrators.length - 1){
        currentMaster = 0;
    } else {
        currentMaster++;
    }
}

//Every sec gets a random value
setInterval(function() {
    var valueToSend = alreadySentValues.pop();

    var options = {
        method: 'GET',
        uri: orchestrators[currentMaster] + '/get/' + valueToSend.key
    };
    rp(options)
        .then(function(res) {
            if (!(randomValues[valueToSend.key] == res.value)) {
                logger.log("Expecting value " + randomValues[valueToSend.key] +
                    " for key " + valueToSend.key);
            } else {
                logger.log("Correctly retrieved key " + valueToSend.key);
            }
        })
        .catch(function(err) {
            logger.warn("Error calling orchestrator " + orchestrators[currentMaster] + ": " + err);
            getNextMaster();
        });

}, 1000);




//Every sec gets a random value
setInterval(function() {
    var valueToSend = alreadySentValues.pop();

    var options = {
        method: 'GET',
        uri: orchestrators[currentMaster] + '/get/' + 'gt' + '/' +  valueToSend.value
    };  
    rp(options)
        .then(function(res) {
            //Hacer algo con los valores recibidos;
            var bod = JSON.parse(res);
            logger.info("Correct multi-value get, got " + bod.results.length  + " results");
        })
        .catch(function(err) {
            logger.warn("Error calling orchestrator " + orchestrators[currentMaster] + ": " + err);
            getNextMaster();
        });

}, 10000);
