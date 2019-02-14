"use strict";

const { DateTime } = require("luxon");

const outputMap = [
  "prb_id",
  "timestamp",
  "minRtt",
  "tick",
  "drift",
  "outOfBand"
];

const rttMap = {
  s: ".",
  m: ",",
  l: "-",
  xl: "+"
};

const statusMap = {
  ok: 0,
  timeout: 1,
  missing: 2,
  error: 3
};

const MAX_TICKS = 5000;

// const createOutputArray = value => [value[1], value[2]];
const createOutputArray = value => value;

const getStatusOkOrError = rta => {
  return (
    (!Number.isFinite(rta[outputMap.indexOf("minRtt")]) && statusMap.error) ||
    statusMap.ok
  );
};

const transformToTickArray = msmMetaData => value => {
  // Note that both the following states will
  // result from this:
  // 1. All attempts returned an rtt, take the minimum
  // 2. At least one attempt returned an rtt, take that one
  // 3. No attempts returned a rtt, return Infinity
  // 4. A field called error was returned in the result (probably "Network unreachable"), also return Infinity
  const minRtt = Math.min(
    ...value.result.map(r => {
      if (Number.isNaN(parseInt(r.rtt))) {
      }
      return (!Number.isNaN(parseInt(r.rtt)) && r.rtt) || Infinity;
    })
  );

  // Calculate what the tick of this result is. Tick
  //  as `tick` intervals from the start time of the measurements.
  //  Then verify that the result is within the bounds of the spread + probe jitter bounds.
  const tick = Math.round(
    (value.timestamp - msmMetaData.start) / msmMetaData.interval
  );
  const drift =
    value.timestamp - (msmMetaData.start + msmMetaData.interval * tick);
  const outOfBand =
    Math.abs(drift) + msmMetaData.probe_jitter > msmMetaData.spread;
  // return [value.prb_id, value.timestamp, minRtt, tick, drift, outOfBand];
  return [value.prb_id, value.timestamp, minRtt, tick, drift, outOfBand];
};

const reduceValidTicks = interval => rttArray => {
  if (!rttArray.length) {
    return [];
  }
  process.stdout.write(`[ ticks in file ${rttArray.length}]`);
  const minRttField = outputMap.indexOf("minRtt");
  const tickField = outputMap.indexOf("tick");
  const bI = rttArray[0][tickField];
  let fillAr = [];
  let ci = 0;

  /*
   *
   *      iOff(=ci +i - offsetStart)   0                                            numberOfTicks + ci
   *                                   +------------------+  +----------------++-------------+
   *                                   |                                                     |
   *                              0    offsetStart         (ci+i)++                rttArray.length + ci
   *                                   +-----------------------------------------------------+
   *                              |    |                                                   |
   *      ci                                  0                     2                1
   *                              +-----------------------+  +----------------++-----------+
   *                              |                       |  |                ||           |
   *       i 0                    bI
   *rttArray +---------------------------------------------mm-----------------d------------+
   *       calculated            start                                                    end
   *       start of              of                                                       of
   *       msm                   this timeslice                                           timeslice
   *
   * ci is not known at for loop creation time, therefore the loop goes to 11.
   * ci is increased when a missing tick is encountered
   *
   * but gets aborted when it reached the limit of numberOfTicks written ticks.
   */

  let offsetStart, numberOfTicks;

  if (rttArray.length <= MAX_TICKS) {
    offsetStart = 0;
    numberOfTicks = rttArray.length;
  } else {
    offsetStart = rttArray.length - MAX_TICKS - 1;
    numberOfTicks = MAX_TICKS;
  }

  console.log(`first index: ${offsetStart}`);
  console.log(`last Index: ${numberOfTicks + offsetStart - 1}`);
  console.log(`number of ticks : ${numberOfTicks}`);
  let timeStampsBuf = new ArrayBuffer(numberOfTicks * 4);
  let timeStampsArr = new Uint32Array(timeStampsBuf);
  let rttBuf = new ArrayBuffer(numberOfTicks * 8);
  let rttArr = new Float64Array(rttBuf);
  let statusBuf = new ArrayBuffer(numberOfTicks);
  let statusArr = new Uint8Array(statusBuf);

  for (let i = offsetStart; i < numberOfTicks + offsetStart - 1; i++) {
    let rta = rttArray[i];
    if (!rta) {
      process.stdout.write(`[no ${i}]`);
    }
    let ri = ci + i + bI;
    let t = rta[tickField];
    let nextTick = rttArray[i + 1];
    let nextT = nextTick && nextTick[tickField];
    let iOff = i - offsetStart + ci;

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
      [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
        rta[outputMap.indexOf("timestamp")],
        rta[outputMap.indexOf("minRtt")],
        getStatusOkOrError(rta)
      ];
      continue;
    }

    // double tick, e.g. 1,1
    if (ri === t && t === nextT) {
      process.stdout.write("d");
      // 1. pick the first run, if it didn't timeout
      // 2. pick the second one if it didn' timeoue
      // 3. pick the first one anyway
      if (Number.isFinite(rta[minRttField])) {
        fillAr.push([...createOutputArray(rta), `doubletick`]);
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          rta[outputMap.indexOf("timestamp")],
          rta[outputMap.indexOf("minRtt")],
          getStatusOkOrError(rta)
        ];
        ci--;
        iOff--;
      } else if (Number.isFinite(nextTick[minRttField])) {
        fillAr.push([...createOutputArray(nextTick), `doubletick`]);
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          nextTick[outputMap.indexOf("timestamp")],
          nextTick[outputMap.indexOf("minRtt")],
          getStatusOkOrError(nextTick)
        ];
        ci--;
        iOff--;
      } else {
        fillAr.push([...createOutputArray(rta), `doubletick`]);
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          rta[outputMap.indexOf("timestamp")],
          rta[outputMap.indexOf("minRtt")],
          getStatusOkOrError(rta)
        ];
        ci--;
        iOff--;
      }
      // skip the next tick, since we're either
      // discarding this tick or the next tick
      // this is the only reason I'm using a loop btw
      // instead of forEach()
      i++;
      continue;
    }

    // gap, e.g. 1,3 or 1,4
    if (t + 1 < nextT) {
      // fillAr.push(createOutputArray(rta).concat(["x"]));
      // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
      //   rta[outputMap.indexOf("timestamp")],
      //   rta[outputMap.indexOf("minRtt")],
      //   getStatusOkOrError(rta)
      // ];
      // ci++;
      // iOff++;

      // cycle untill we reach the next tick
      let aiTs, lastAiTs;
      for (let ni = 0; ni < nextT - ri; ni++) {
        // end of the rttArray
        if (!nextTick) {
          continue;
        }
        process.stdout.write("m");
        lastAiTs = (aiTs && aiTs) || rta[outputMap.indexOf("timestamp")];
        aiTs = rta[outputMap.indexOf("timestamp")] + interval * ni;
        //nextTick[outputMap.indexOf("drift")];
        fillAr.push(
          createOutputArray([
            rta[outputMap.indexOf("prb_id")],
            aiTs,
            "missing",
            ni + ri,
            aiTs - lastAiTs
          ])
        );
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          aiTs,
          0,
          statusMap.missing
        ];
        ci++;
        iOff++;
      }
      ci--;
      iOff--;
      continue;
    }

    fillAr.push(createOutputArray(rta));
    [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
      rta[outputMap.indexOf("timestamp")],
      rta[outputMap.indexOf("minRtt")],
      getStatusOkOrError(rta)
    ];
    ci++;
    iOff++;
  }
  // console.log(rttArr);
  return [fillAr, timeStampsArr, rttArr, statusArr];
};

const composeRowsWithMaxTimeStamp = (transform, reduce) => rowData => {
  let maxTimeStamp = 0;
  const tsField = outputMap.indexOf("timestamp");
  const r = rowData.reduce((resultData, row) => {
    // columnValues holds the different versions
    row.columnValues.forEach(cv => {
      const ra = transform(JSON.parse(cv.value.toString()));
      resultData.push(ra);
      // keep the maximum time stamp found in all results.
      maxTimeStamp =
        (ra[tsField] > maxTimeStamp && ra[tsField]) || maxTimeStamp;
    });
    return resultData;
  }, []);
  return reduce(r);
};

const transduceResultsToTicks = msmMetaData => {
  return composeRowsWithMaxTimeStamp(
    transformToTickArray(msmMetaData),
    reduceValidTicks(msmMetaData.interval)
  );
};

module.exports = { transduceResultsToTicks };
