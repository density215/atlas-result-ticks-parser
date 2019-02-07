const thrift = require("thrift"),
    Service = require("./0.9.3/THBaseService"),
    ttypes = require("./0.9.3/hbase_types"),
    //thriftPool = require("node-thrift-pool"),
    util = require("util");

THRIFT_HOST = "thrift.flow.ripe.net";

THRIFT_PORT = 9091;
TIMEOUT = 2500;
KEEPALIVE = 600;

var transport = thrift.TBufferedTransport;
var protocol = thrift.TBinaryProtocol;

var thrift_options = {
    timeout: TIMEOUT,
    keepalive: KEEPALIVE
};

//var thrift_client = thriftPool(thrift, Service, {
//  host: THRIFT_HOST,
//  port: THRIFT_PORT
//});

var connection = thrift.createConnection(THRIFT_HOST, THRIFT_PORT, {
    transport: transport,
    protocol: protocol
});

connection.on("error", function(err) {
    console.log("error opening connection");
    console.log(err);
});

// Create a Calculator client with the connection
var thrift_client = thrift.createClient(Service, connection);

//scan 'atlas-blobs',{COLUMNS=>['-:50208'],STARTROW=>'msm:18725407|ts:2019-02-04T',STOPROW=>'msm:18725407|ts:2019-02-04T~'}
//get 'atlas-blobs','msm:5151|ts:2019-02-04T23:50', {COLUMNS=>['-:50208']}
var column = new ttypes.TColumn({
    family: "-",
    qualifier: "50208"
});

var get = new ttypes.TGet({
    row: "msm:5151|ts:2019-02-04T23:50",
    columns: column
});

console.log("open scanner...");
var scan = new ttypes.TScan({
    startRow: "msm:18725407|ts:2019-02-04T",
    stopRow: "msm:18725407|ts:2019-02-04T~",
    columns: [column],
    maxVersions: 1
});

console.log("start reading rows...");
thrift_client.openScanner("atlas-blobs", scan, (err, scannerId) => {
    if (err) {
        console.log("error opening scanner");
        console.error(err);
    } else {
        console.log("thrift responded!");
        console.log(scannerId);

        thrift_client.getScannerRows(scannerId, 1, (err, rows) => {
            if (err) {
                console.log("error reading rows");
                console.error(err);
            }

            if (rows.length) {
                rows.forEach(row => {
                    const resp = row.columnValues.map(cv =>
                        JSON.parse(cv.value.toString())
                    );
                    console.log(
                        util.inspect(resp, { showHidden: false, depth: null })
                    );
                });
            } else {
                thrift_client.closeScanner(scannerId);
                thrift_client.end_connection();
            }
        });
        //const resp = response.columnValues.map(cv =>
        //  JSON.parse(cv.value.toString())
        //);
        // console.log(util.inspect(resp, { showHidden: false, depth: null }));
        return scannerId;
    }
});
