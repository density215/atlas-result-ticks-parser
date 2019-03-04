"use strict";

const { DateTime } = require("luxon");

const ticksArrayType = [
  "timestamp",
  "tick",
  "minRtt",
  "status",
  "statusMsg",
  "drift",
  "outOfBand"
];

const getTickProp = fieldName => ticksArrayType.indexOf(fieldName);

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

// const createOutputArray = value => [value[3]];
const createOutputArray = value => [value[1], value[3], value[4]];

const transformToTickArray = msmMetaData => value => {
  // Note that both the following states will
  // result from this:
  // 1. All attempts returned an rtt, take the minimum
  // 2. At least one attempt returned an rtt, take that one
  // 3. If No attempts returned a rtt, check if there's a timeout (`{ "x": "*"}` in RIPE Atlas results) and return that
  // 4. Return the `error` field (`[{"error": "...."}])` in RIPE Atlas results)
  // 5. If none of the above conditions are met [Infinity,""] is returned

  // takes: msmMetaData: metadata retrieved from the RIPE Atlas measurements API.
  //        value: rows output of the hbase scan.

  // returns: [minRtt || Infinity, <ErrorMSg>, <statusMap>]. If a minRtt is present then Error can be ignored
  // (meaning one of the values contained an error/timeout, but there's also a rtt)
  const cleanRtt = value.result.reduce(
    ([minRtt, errMsg, status], r) => {
      const rttNum = parseInt(r.rtt);
      let rttStatus =
        status === statusMap.ok
          ? statusMap.ok
          : (Number.isFinite(rttNum) && statusMap.ok) ||
            (r.x && r.x === "*" && statusMap.timeout) ||
            statusMap.error;

      return (
        (Number.isFinite(rttNum) &&
          minRtt > r.rtt && [r.rtt, null, statusMap.ok]) || [
          minRtt,
          `${(errMsg && errMsg) || ""}${(r.x && r.x) ||
            (r.error && r.error) ||
            ""}`,
          rttStatus
        ]
      );
    },
    [Infinity, null, null]
  );

  // If the first element holds a Number then that's the minRtt, otherwise
  // the second element should hold an error.
  // Note that the second element can hold an error if the first is a Number
  const minRtt = (Number.isFinite(cleanRtt[0]) && cleanRtt[0]) || null;

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
  // this output data structure should conform to the output 'type' of outputMap!
  return [
    value.timestamp,
    tick,
    minRtt,
    cleanRtt[2],
    (cleanRtt[1] && cleanRtt[1]) || null, // filter out empty string
    tick,
    drift,
    outOfBand
  ];
};

// note that msmMetaData also contains probe_jitter, prbId
const reduceValidTicks = msmMetaData => ticksArray => {
  // takes
  // msmMetaData: metadata from the RIPE Atlas measurements API
  // (curried) ticksArray: array of ticks in the ticksArrayType
  // items in the array should use as getter: getTickProp(<FIELDNAME>)

  if (!ticksArray.length) {
    return [];
  }
  console.log("\n-+-+-+-+-+-+-+-+-");
  process.stdout.write(`[start probe ${msmMetaData.prbId}]`);
  process.stdout.write(`[ ticks in file ${ticksArray.length}]`);
  const minRttField = getTickProp("minRtt");
  const tickField = getTickProp("tick");
  const bI = ticksArray[0][tickField];
  let fillAr = [];
  let ci = 0;

  /*
   *                                                   extactTicks
   *                              +----------------------------------------------------------+
   *                              |                                                          |
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
   * exactTicks is the calculates number of ticks based on the duration of the timeslice and the interval.
   *
   * ci is not known at for loop creation time, therefore the loop goes to 11.
   * ci is increased when a missing tick is encountered
   *
   * but gets aborted when it reached the limit of numberOfTicks written ticks.
   */

  let offsetStart, numberOfTicks;

  // if (rttArray.length <= msmMetaData.exactTicks) {
  //   offsetStart = 0;
  //   numberOfTicks = rttArray.length;
  // } else {
  //   offsetStart = rttArray.length - msmMetaData.exactTicks - 1;
  //   numberOfTicks = msmMetaData.exactTicks;
  // }
  offsetStart = 0;
  numberOfTicks = msmMetaData.exactTicks;

  process.stdout.write(`[first index: ${offsetStart}]`);
  process.stdout.write(`[last Index: ${numberOfTicks + offsetStart - 1}]`);
  process.stdout.write(`[number of ticks : ${numberOfTicks}]`);
  let timeStampsBuf = new ArrayBuffer(numberOfTicks * 4);
  let timeStampsArr = new Uint32Array(timeStampsBuf);
  let rttBuf = new ArrayBuffer(numberOfTicks * 8);
  let rttArr = new Float64Array(rttBuf);
  let statusBuf = new ArrayBuffer(numberOfTicks);
  let statusArr = new Uint8Array(statusBuf);

  for (let i = offsetStart; i < numberOfTicks + offsetStart - 1; i++) {
    let rta = ticksArray[i];
    if (!rta) {
      // probably the end of the rttArray
      process.stdout.write(`[no ${i}]`);
      continue;
    }
    let ri = i + ci + bI;
    let t = rta[tickField];
    let nextTick = ticksArray[i + 1];
    let nextT = nextTick && nextTick[tickField];
    let iOff = i + ci - offsetStart;

    // normal order e.g. 1,2
    if (ri === t && t + 1 === nextT) {
      if (rta[minRttField] < 10) {
        process.stdout.write(rttMap["s"]);
      } else if (rta[minRttField] < 50) {
        process.stdout.write(rttMap["m"]);
      } else if (rta[minRttField] < 100) {
        process.stdout.write(rttMap["l"]);
      } else if (Number.isFinite(rta[minRttField])) {
        process.stdout.write(rttMap["xl"]);
      } else {
        process.stdout.write("x");
      }
      fillAr.push(createOutputArray(rta));
      [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
        rta[getTickProp("timestamp")],
        rta[getTickProp("minRtt")],
        rta[getTickProp("status")]
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
        fillAr.push([...createOutputArray(rta), `doubletick1`]);
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          rta[getTickProp("timestamp")],
          rta[getTickProp("minRtt")],
          rta[getTickProp("status")]
        ];
        ci--;
        // iOff--;
      } else if (Number.isFinite(nextTick[minRttField])) {
        fillAr.push([...createOutputArray(nextTick), `doubletick2`]);
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          nextTick[getTickProp("timestamp")],
          nextTick[getTickProp("minRtt")],
          nextTick[getTickProp("status")]
        ];
        ci--;
        // iOff--;
      } else {
        fillAr.push([...createOutputArray(rta), `doubletick3`]);
        [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
          rta[getTickProp("timestamp")],
          rta[getTickProp("minRtt")],
          rta[getTickProp("status")]
        ];
        ci--;
        // iOff--;
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
      // cycle untill we reach the next tick
      let aiTs, lastAiTs;
      for (let ni = 0; ni < nextT - ri; ni++) {
        // end of the rttArray
        if (!nextTick) {
          continue;
        }

        process.stdout.write("m");
        lastAiTs = (aiTs && aiTs) || rta[getTickProp("timestamp")];
        aiTs = rta[getTickProp("timestamp")] + msmMetaData.interval * ni;
        //nextTick[outputMap.indexOf("drift")];
        fillAr.push(
          createOutputArray([
            aiTs, // timeStamp
            ni + ri, // tick
            null, // minRtt
            statusMap.missing, // status
            "missing", // statusMsg
            aiTs - lastAiTs, // drift
            false // outOfBand
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
      // iOff--;
      continue;
    }

    // fillAr.push([...createOutputArray(rta), "leftover"]);
    [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
      rta[getTickProp("timestamp")],
      rta[getTickProp("minRtt")],
      rta[getTickProp("status")]
    ];
    // ci++;
    // iOff++;
  }
  // console.log(rttArr);
  return [
    fillAr,
    timeStampsArr.subarray(0, fillAr.length),
    rttArr.subarray(0, fillAr.length),
    statusArr.subarray(0, fillAr.length)
  ];
};

const composeRowsWithMaxTimeStamp = (transform, reduce) => rowData => {
  let maxTimeStamp = 0,
    minTimeStamp = Infinity;
  const tsField = getTickProp("timestamp");
  const minRttField = getTickProp("minRtt");
  const r = rowData.reduce((resultData, row) => {
    // columnValues holds the different versions
    row.columnValues.forEach(cv => {
      const ra = transform(JSON.parse(cv.value.toString()));
      resultData.push(ra);
      // keep the maximum time stamp found in all results.
      maxTimeStamp =
        (ra[minRttField] < Infinity &&
          ra[tsField] > maxTimeStamp &&
          ra[tsField]) ||
        maxTimeStamp;
      minTimeStamp =
        (ra[minRttField] < Infinity &&
          ra[tsField] < minTimeStamp &&
          ra[tsField]) ||
        minTimeStamp;
    });
    return resultData;
  }, []);
  return [
    reduce(r),
    (Number.isFinite(minTimeStamp) && minTimeStamp) || null,
    (maxTimeStamp && maxTimeStamp) || null
  ];
};

const transduceResultsToTicks = msmMetaData => {
  return composeRowsWithMaxTimeStamp(
    transformToTickArray(msmMetaData),
    reduceValidTicks(msmMetaData)
  );
};

module.exports = { transduceResultsToTicks };
