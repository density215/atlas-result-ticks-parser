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
const startTime = now.minus({ weeks: 2 }).startOf("day");

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

const outputMap = [
    "prb_id",
    "timestamp",
    "minRtt",
    "tick",
    "drift",
    "outOfBand"
];

const createOutputArray = value => [value[1], value[2]];

const rttMap = {
    s: ".",
    m: ",",
    l: "-",
    xl: "+"
};

let minRttData = {};
let maxTimeStamp = 0;

const probeScan = (msmId, prbId, startTime, stopTime) => {
    const scan = new HBase.Scan();
    //scan 'atlas-blobs',{COLUMNS=>['-:50208'],STARTROW=>'msm:18725407|ts:2019-02-04T',STOPROW=>'msm:18725407|ts:2019-02-04T~'}
    scan.setStartRow(`msm:${msmId}|ts:${startTime.toFormat(dateKeyFormat)}`); //start rowKey
    // console.log(`msm:${msmId}|ts:${startTime.toFormat(dateKeyFormat)}`);
    scan.setStopRow(`msm:${msmId}|ts:${stopTime}~`); //stop rowKey
    // console.log(`msm:${msmId}|ts:${stopTime}~`);
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
                // process.stdout.write(r.x || (r.error && "E") || "?");
                // console.log("infin");
            }
            return (!Number.isNaN(parseInt(r.rtt)) && r.rtt) || Infinity;
        })
    );

    // if (!Number.isFinite(minRtt)) {
    //     process.stdout.write("I");
    // } else {
    //     process.stdout.write(".");
    // }

    // Calculate what the tick of this result is. Tick
    //  as `tick` intervals from the start time of the measurements.
    //  Then verify that the result is within the bounds of the spread + probe jitter bounds.
    const tick = Math.round((value.timestamp - msmStart) / interval);
    const drift = value.timestamp - (msmStart + interval * tick);
    const outOfBand = Math.abs(drift) + probe_jitter > spread;
    // return [value.prb_id, value.timestamp, minRtt, tick, drift, outOfBand];
    return [value.prb_id, value.timestamp, minRtt, tick, drift, outOfBand];
};

const validateTicks = rttArray => {
    const exactTicks = DateTime.fromSeconds(maxTimeStamp)
        .diff(startTime, "seconds")
        .toObject()["seconds"];
    process.stdout.write(
        `[ ticks in file ${rttArray.length} calculated ticks ${Math.floor(
            exactTicks / interval
        )}]`
    );
    const minRttField = outputMap.indexOf("minRtt");
    const tickField = outputMap.indexOf("tick");
    const bI = rttArray[0][tickField];
    let fillAr = [];
    let ci = 0;
    // let i = 0;
    rttArray.forEach((rta, i) => {
        const ri = ci + i + bI;
        const t = rta[tickField];
        const nextTick = rttArray[i + 1];
        const nextT = nextTick && nextTick[tickField];

        // normal order e.g. 1,2
        if (ri === t && t + 1 === nextT) {
            if (rta[minRttField] < 10) {
                process.stdout.write(rttMap["s"]);
            } else if (rta[minRttField] < 50) {
                process.stdout.write(rttMap["m"]);
            } else if (rta[minRttField] < 100) {
                process.stdout.write(rttMap["l"]);
            } else {
                process.stdout.write(rttMap["xl"]);
            }
            fillAr.push(createOutputArray(rta));
            return;
        }

        // double tick, e.g. 1,1
        if (ri === t && t === nextT) {
            process.stdout.write("d");
            // 1. pick the first run, if it didn't timeout
            // 2. pick the second one if it didn' timeoue
            // 3. pick the first one anyway
            if (Number.isFinite(rta[minRttField])) {
                fillAr.push([...createOutputArray(rta), `doubletick`]);
            } else if (Number.isFinite(nextTick[minRttField])) {
                fillAr.push([...createOutputArray(nextTick), `doubletick`]);
            } else {
                fillAr.push([...createOutputArray(rta), `doubletick`]);
            }
            // ci--;
            ci++;
            return;
        }

        // gap, e.g. 1,3 or 1,4
        if (t + 1 < nextT) {
            // console.log(`${bI} ${ci} ${ri} -> <- ${t}`);
            fillAr.push(createOutputArray(rta));
            // cycle untill we reach the next tick
            for (var i = ri + 1; i < nextT; i++) {
                process.stdout.write("m");
                fillAr.push(
                    createOutputArray([
                        rta[outputMap.indexOf("prb_id")],
                        rta[outputMap.indexOf("timestamp")] +
                            interval * (i - ri),
                        "missing",
                        i
                    ])
                );
                ci++;
            }
        }
    });
    // console.log(fillAr);
    return fillAr;
};

const outputBlob = rows => {
    // console.log(`no of rows: ${rows.length}`);
    const tsField = outputMap.indexOf("timestamp");
    const r = rows.reduce((resultData, row) => {
        // console.log(row.columnValues);

        // columnValues holds the different versions
        row.columnValues.forEach(cv => {
            // console.log(cv.value.toString());
            const ra = fieldFilter(JSON.parse(cv.value.toString()));
            resultData.push(ra);
            if (ra[tsField] > maxTimeStamp) {
                maxTimeStamp = ra[tsField];
            }
            // return resultData;
        });
        // console.log(resultData);
        return resultData;
    }, []);
    return r;
};

console.log(`measurement :\t${msmId}`);
console.log(`timespan :\t${startTime.toFormat(dateKeyFormat)} - ${stopTime}`);

probeIds
    // .filter(prbId => prbId === 33174)
    // .slice(0, 11)
    .forEach((prbId, idx, probeIdsArray) => {
        const scan = probeScan(msmId, prbId, startTime, stopTime);
        HBase.createScanStream(table, scan)
            .on("data", rows => {
                // process.stdout.write(`prb: ${prbId}\n`);
                const r = outputBlob(rows);
                minRttData[prbId] =
                    (minRttData[prbId] && minRttData[prbId].concat(r)) || r;
                // console.log(f);
                // console.log(r);
            })
            .on("error", err => {
                console.log(err);
            })
            .on("end", () => {
                if (!minRttData[prbId] || minRttData[prbId].length === 0) {
                    console.log(`empty set :\t ${prbId}`);
                    return;
                } else {
                    process.stdout.write(`[write msm ${msmId} prb ${prbId}}]`);
                }
                const r = validateTicks(minRttData[prbId]);
                const csvWriter = createCsvWriter({
                    path: `result_data/msm_${msmId}_${prbId}.csv`,
                    header: csvHeader,
                    append: true
                });
                csvWriter
                    .writeRecords(r) // returns a promise
                    .then(() => {
                        // process.stdout.write(".");
                        process.stdout.write(
                            `[done msm ${msmId} prb ${prbId}. wrote ${
                                minRttData[prbId].length
                            }]\n`
                        );
                        if (idx + 1 === probeIdsArray.length) {
                            console.log("[exit]");
                            process.exit();
                        }
                    });
            });
    });
