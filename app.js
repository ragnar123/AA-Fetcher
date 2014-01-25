// Dependencies
var http = require('http'),
    async = require('async');
    pg = require('pg'),
    xml2js = require('xml2js'),
    config = require('./config.js'),
    client = new pg.Client(config.conString);


// Classes
function Flight(type, obj) {
// Example object:
//  { Date: [ '26-01' ],
//  Planned: [ '20:20' ],
//  Expected: [ '' ],
//  Route: [ 'RC459' ],
//  From: [ 'Copenhagen' ],
//  Remark: [ '' ],
//  LongRemark: [ '' ] }

    this.planned_time = new Date("2014-" + obj.Date[0].split('-').reverse().join('-') + " " + obj.Planned[0]);
    this.expected_time = (obj.Expected[0].length > 0) ? new Date("2014-" + obj.Date[0].split('-').reverse().join('-') + " " + obj.Expected[0]) : null;
    this.route = obj.Route[0];
    this.from = obj.From[0];

    this.type = type;

    // Returns parameterized query and a array of values
    this.getInsertQuery = function () {
        return {
            query: 'INSERT INTO flights (planned_time, expected_time, route, "from", "type") VALUES ( $1, $2, $3, $4, $5 )',
            values: [this.planned_time, this.expected_time, this.route, this.from, this.type]
        }
    }
}


// Functions used in flow
function getFlightXml(callback) {
    http.get("http://crewportal.atlantic.fo/sms/Flightinfoxml.php", function(res) {
        res.on('data', function(data) {
            callback(null, data);
        });
    }).on('error', function(e) {
        // Log error and notify admin
        // If error persists, investigate and contact data provider
        callback(e, null);
    });
}

function parseXml(xmlString, callback) {
    xml2js.parseString(xmlString, function (err, result) {
        var flights = [],
            arrivals = result.AtlanticAirwaysFlightsVagar.Arrivals[0].Arrival,
            departures = result.AtlanticAirwaysFlightsVagar.Departures[0].Departure;

        arrivals.forEach(function (arrival) {
            flights.push(new Flight('arrival', arrival));
        });

        departures.forEach(function (departure) {
            flights.push(new Flight('departure', departure));
        });

        callback(err, flights);
    });
}

function insertFlights(flights, callback) {
    console.log(flights);
}

async.waterfall([
        // Retrieves list of flights from atlantic.fo
        getFlightXml,
        // Parses the xml file to js objects. Returns an array of flight objects
        parseXml,
        // Checks for duplicate entries and insertes new flights
        insertFlights
    ], function (err, result) {
        console.log(result);
    });

client.connect(function(err) {
    if (err) {
        return console.error('could not connect to postgres', err);
    }

    client.query('SELECT NOW() AS "theTime"', function (err, result) {
        if (err) {
            return console.error('error running query', err);
        }
        console.log(result.rows[0].theTime);
        client.end();
    });
});

