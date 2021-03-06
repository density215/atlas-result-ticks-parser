import HBase from "node-thrift2-hbase";
import { transduceResultsToTicksWithTypedArrays } from "./transformers";

/*
 * ERROR HANDLING
 *
 * Either you can:
 * 1) pass the standard errHandler function listed below in the .catch() on a fetch OR
 * 2) define a custom error and throw that.
 *
 * custom error object definition:
 *
 * {
 *    status: <HTTPStatusCode::<Number> || "customErr"::<String>>
 *    detail: <String>
 * }
 *
 * `status` is the HTTP status code that you can pass on from a network error from fetch or the special
 * variant "customErr", in which case a special message stating an internal error will be displayed to
 * the user.
 *
 * `detail` is the message for the user that will be displayed below the generic error message.
 * please do not repeat the generic error message ("404. file not found") but try to be as specific
 * as possible.
 */

const config = {
  hosts: ["thrift.flow.ripe.net"],
  port: "9091"
};

const HBaseAdapter = HBase(config);
const HBaseTables = {
  blobs: "atlas-blobs"
};

export const hbaseMsmProbeTimeRangeScan = async ({
  msmMetaData,
  prbId,
  startTime, // startTime as scan string, like '2019-02-04T14:00' or '2019-02-04T' (from beginning of day)
  stopTime, // stopTime as scan string, like '2019-02-05T15:00' or '2019-02-05T' (until end of day)
  transducer = null // user can use a custom transducer
}) => {
  const scan = new HBaseAdapter.Scan();
  //scan 'atlas-blobs',{COLUMNS=>['-:50208'],STARTROW=>'msm:18725407|ts:2019-02-04T',STOPROW=>'msm:18725407|ts:2019-02-04T~'}
  scan.setStartRow(`msm:${msmMetaData.msmId}|ts:${startTime}`); //start rowKey
  scan.setStopRow(`msm:${msmMetaData.msmId}|ts:${stopTime}~`); //stop rowKey
  scan.add("-", `${prbId}`); //scan family and qualifier info:name
  scan.setChunkSize(10);
  scan.setMaxVersions(99999);

  msmMetaData.prbId = prbId;
  const downloadStart = new Date();
  console.log("[start thrift request]");

  let response = await hbaseScanStream({
    table: HBaseTables.blobs,
    scan: scan,
    transduce:
      (transducer && transducer(msmMetaData)) ||
      transduceResultsToTicksWithTypedArrays(msmMetaData)
  });

  console.log(`[thrift request time (ms) : ${new Date() - downloadStart}]`);
  return response;
};

/* 
This function wraps the createScanStream from thrift2-hbase module
it allows for a `transduce` function that transforms & reduces the result
if it is omitted it will return the result directly as a TypedArray.
Note that it will return a Promise.resolve if all chunks are loaded.

arguments:

table        HBase table
scan         TScan (made with hbase-thrift2 Scan object)
transduce    function that takes the result and gets run once over the whole result

returns a Promise that wraps the result of the complete scan
*/
const hbaseScanStream = async ({ table, scan, transduce }) => {
  return new Promise((resolve, reject) => {
    let resultArr = [];
    HBaseAdapter.createScanStream(table, scan)
      .on("data", rows => {
        resultArr = resultArr.concat(rows);
      })
      .on("error", err => {
        console.log(err);
        reject(err);
      })
      .on("end", () => {
        const r =
          (resultArr.length && transduce && transduce(resultArr)) || resultArr;
        return (
          (r.length && resolve(r)) ||
          ((r &&
            reject({ status: "404", detail: "No data (no rows found)" })) ||
            reject({
              status: "500",
              detail: "Error while trying to perform HBase scan"
            }))
        );
      });
  });
};
