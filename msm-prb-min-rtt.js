"use strict";

const config = {
    hosts: ["thrift.flow.ripe.net"],
    port: "9091"
};

const csvHeader = ["ts", "prb_id"];
const dateKeyFormat = "yyyy-LL-dd'T'";

const { DateTime } = require("luxon");
const HBase = require("node-thrift2-hbase")(config);
const createCsvWriter = require("csv-writer").createArrayCsvWriter;

/* stupid hardcoded stuff for now */
const msmId = 18725407;
const msmStart = 1546503240;
const now = DateTime.local();
const stopTime = now.toFormat(dateKeyFormat);
const startTime = now.minus({ weeks: 2 }).toFormat(dateKeyFormat);

const probeIds = [
    116,
    1217,
    365,
    579,
    746,
    1107,
    1320,
    3791,
    4649,
    4873,
    10176,
    10338,
    10446,
    10688,
    11180,
    11266,
    11902,
    12077,
    12406,
    12475,
    12754,
    12918,
    12975,
    13104,
    13694,
    13808,
    14366,
    14511,
    14547,
    14814,
    15771,
    16005,
    16688,
    16950,
    17356,
    17914,
    18205,
    18339,
    18951,
    20171,
    20305,
    20326,
    20446,
    20484,
    20485,
    21120,
    21256,
    21390,
    21860,
    22637,
    22668,
    22859,
    22897,
    23961,
    23984,
    24244,
    24886,
    25389,
    25965,
    25982,
    26346,
    27507,
    27720,
    28095,
    28303,
    28516,
    28709,
    28715,
    29833,
    29852,
    30060,
    30259,
    30303,
    30381,
    30384,
    30871,
    30955,
    31222,
    31450,
    31489,
    32083,
    32194,
    32293,
    32534,
    32663,
    32763,
    32890,
    32959,
    33174,
    33446,
    34132,
    34244,
    34260,
    34302,
    34397,
    34405,
    34754,
    34801,
    35562,
    50467,
    50482
];
const interval = 240;
const spread = 120;
const probe_jitter = 3;
const table = "atlas-blobs";
/* end of the hardcoded stuff */

const probeScan = (msmId, prbId, startTime, stopTime) => {
    const scan = new HBase.Scan();
    //scan 'atlas-blobs',{COLUMNS=>['-:50208'],STARTROW=>'msm:18725407|ts:2019-02-04T',STOPROW=>'msm:18725407|ts:2019-02-04T~'}
    scan.setStartRow(`msm:${msmId}|ts:${startTime}`); //start rowKey
    console.log(`msm:${msmId}|ts:${startTime}`);
    scan.setStopRow(`msm:${msmId}|ts:${stopTime}~`); //stop rowKey
    console.log(`msm:${msmId}|ts:${stopTime}~`);
    scan.add("-", `${prbId}`); //scan family and qualifier info:name
    scan.setChunkSize(10);
    scan.setMaxVersions(99999);

    return scan;
};

// scan 'atlas-latest',{COLUMNS=>'-:blob',STARTROW=>'msm:1001407|prb:1',LIMIT=>100,FILTER=>"(PrefixFilter('msm:1001407|')"}
// scan.setStartRow("msm:1001407|prb:1");
// scan.setChunkSize(100);
// scan.add("-", "blob");
// scan.setFilterString("(PrefixFilter('msm:1001407|')");
// scan.setMaxVersions(1);

const fieldFilter = value => {
    // Note that both the following states will
    // result from this:
    // 1. All attempts returned an rtt, take the minimum
    // 2. At least one attempt returned an rtt, take that one
    // 3. No attempts returned a rtt, return Infinity
    // 4. A field called error was returned in the result (probably "Network unreachable"), also return Infinity
    const minRtt = Math.min(
        ...value.result.map(r => {
            if (Number.isNaN(parseInt(r.rtt))) {
                process.stdout.write(r.x || (r.error && "E") || "?");
                // console.log("infin");
            }
            return (!Number.isNaN(parseInt(r.rtt)) && r.rtt) || Infinity;
        })
    );

    if (!Number.isFinite(minRtt)) {
        process.stdout.write("I");
    }

    // Calculate what the tick of this result is. Tick
    //  as `tick` intervals from the start time of the measurements.
    //  Then verify that the result is within the bounds of the spread + probe jitter bounds.
    const tick = Math.round((value.timestamp - msmStart) / interval);
    const drift = value.timestamp - (msmStart + interval * tick);
    const outOfBand = Math.abs(drift) + probe_jitter > spread;
    return [value.prb_id, value.timestamp, minRtt, tick, drift, outOfBand];
};

const outputBlob = rows => {
    // console.log(`no of rows: ${rows.length}`);
    const r = rows.reduce((resultData, row) => {
        // console.log(row.columnValues);

        // columnValues holds the different versions
        row.columnValues.forEach(cv => {
            // console.log(cv.value.toString());
            resultData.push(fieldFilter(JSON.parse(cv.value.toString())));
            // return resultData;
        });
        // console.log(resultData);
        return resultData;
    }, []);
    return r;
};

console.log(`measurement :\t${msmId}`);
console.log(`timespan :\t${startTime} - ${stopTime}`);

probeIds.slice(0, 10).forEach(prbId => {
    const scan = probeScan(msmId, prbId, startTime, stopTime);
    HBase.createScanStream(table, scan)
        .on("data", rows => {
            // process.stdout.write(`prb: ${prbId}\n`);

            const f = outputBlob(rows);
            // console.log(f);
            const csvWriter = createCsvWriter({
                path: `result_data/msm_${msmId}_${prbId}.csv`,
                header: csvHeader,
                append: true
            });
            csvWriter
                .writeRecords(f) // returns a promise
                .then(() => {
                    process.stdout.write(".");
                    // console.log(`done for ${msmId} and ${prbId}`);
                });
        })
        .on("error", err => {
            console.log(err);
        })
        .on("end", () => {
            process.stdout.write("\n");
            process.exit();
        });
});
