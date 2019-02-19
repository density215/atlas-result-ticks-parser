"use strict";

const csvHeader = ["ts", "prb_id"];
const dateKeyFormat = "yyyy-LL-dd'T'";

const { DateTime } = require("luxon");
const createCsvWriter = require("csv-writer").createArrayCsvWriter;

const rtthmm = require("/opt/projects/RTTHMM-bindings/node/build/Release/rtthmm");

const { hbaseMsmProbeTimeRangeScan } = require("./src/adapters");

/* stupid hardcoded stuff for now */
const now = DateTime.utc();
const stopTime = now.toFormat(dateKeyFormat);
const startTime = now.minus({ weeks: 2 }).startOf("day");

const msmMetaData = {
  msmId: 18725407,
  start: 1546503240, // start_time of msm
  probeIds: [
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
  ],
  interval: 240,
  spread: 120,
  probe_jitter: 3
};

msmMetaData.exactTicks = Math.floor(
  now.diff(startTime) / 1000 / msmMetaData.interval
);

console.log("----------");
console.log(`measurement :\t${msmMetaData.msmId}`);
console.log(`timespan :\t${startTime.toFormat(dateKeyFormat)} - ${stopTime}`);
console.log(`start time: ${startTime.toUTC()}`);
console.log(`stop time: ${now.toUTC()}`);
console.log(`interval: ${msmMetaData.interval}`);
console.log(`calculated ticks: ${msmMetaData.exactTicks}`);

// ./msm_18725407_10688.csv
// ./msm_18725407_1217.csv
// ./msm_18725407_16688.csv
// ./msm_18725407_18205.csv
// ./msm_18725407_28303.csv
// ./msm_18725407_30060.csv
// ./msm_18725407_32890.csv
// ./msm_18725407_33174.csv
msmMetaData.probeIds
  // .filter(
  //   prbId =>
  //     prbId === 10688 ||
  //     prbId === 1217 ||
  //     prbId === 16688 ||
  //     prbId === 18205 ||
  //     prbId === 28303 ||
  //     prbId === 30060 ||
  //     prbId === 32890 ||
  //     prbId === 33174
  // )
  // .slice(0, 11)
  .forEach((prbId, idx, probeIdsArray) => {
    hbaseMsmProbeTimeRangeScan({
      msmMetaData,
      prbId,
      startTime,
      stopTime
    })
      .then(
        ([[csvArr, tsArr, rttArr, statusArr], minTimeStamp, maxTimeStamp]) => {
          console.log(
            `[csv: ${csvArr.length} ts: ${tsArr.length} rtt: ${
              rttArr.length
            } status: ${statusArr.length}]`
          );
          console.log(
            `[min received timestamp: ${DateTime.fromSeconds(
              minTimeStamp
            ).toUTC()}]`
          );
          console.log(
            `[max received timestamp: ${DateTime.fromSeconds(
              maxTimeStamp
            ).toUTC()}]`
          );

          let statusMatrix = [];
          console.log(`[start hmm for probe ${prbId}]`);
          try {
            statusMatrix = rtthmm.fit(tsArr, rttArr, statusArr);
          } catch (error) {
            console.log("rtthmm crashed");
            console.log(error);
            statusMatrix = new Array(csvArr.length).fill("E");
          }
          console.log(`model length: ${statusMatrix.length}`);
          console.log(`ts length ${tsArr.length}`);
          console.log(tsArr.slice(0, 3));
          console.log(tsArr.slice(tsArr.length - 3));

          const csvWriter = createCsvWriter({
            path: `/Users/jdenhertog/Sandbox/msm-prb-min-rtt/result_data/new/msm_${
              msmMetaData.msmId
            }_${prbId}.csv`,
            header: csvHeader,
            append: false
          });
          csvWriter
            .writeRecords(
              Array.from(csvArr, (s, i) => [
                [...s],
                tsArr[i],
                rttArr[i],
                statusArr[i],
                statusMatrix[i]
              ])
            )
            .then(
              () => {
                process.stdout.write(
                  `[done msm ${msmMetaData.msmId} prb ${prbId}. wrote ${
                    csvArr.length
                  }]\n`
                );
                console.log("-+-+-+-+-+-+-+-+-");
                if (idx + 1 === probeIdsArray.length) {
                  console.log("[exit]");
                  process.exit();
                }
              },
              err => {
                console.log("error writing csv file");
                console.log(err);
                process.exit();
              }
            )
            .catch(err => {
              console.log("error writing csv file");
              console.log(err);
              process.exit();
            });
          console.log(`[end writing probe ${prbId}]`);
        },
        err => {
          switch (err.status) {
            case "500":
              console.log("Error opening HBase scan");
              break;
            case "404":
              console.log("Empty HBase response (no data)");
              break;
            default:
              console.log("Unknown error while trying to open HBase scan");
          }
        }
      )
      .catch(err => {
        console.log(err);
        process.exit(1);
      });
  });
