// This defines the output of the intermediate format that
// yields from the transducer(s).
const ticksArrayType = [
  "timestamp",
  "tick",
  "minRtt",
  "status",
  "statusMsg",
  "drift",
  "outOfBand"
];

// This defines the part of the intermediate format that
// is passed on to the API.

// const createOutputArray = value => [value[3]];
const outputFormat = value => [value[1], value[3], value[4]];

// The public 'interface'
export const getTickProp = fieldName => {
  const iName = ticksArrayType.indexOf(fieldName);
  return iName !== -1 ? iName : null;
};
export const getTicksOutputSchema = outputFormat(ticksArrayType);

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

class AtlasResults extends Array {
  toOutputArrays(outputFormat) {
    return [
      this.map(outputFormat),
      Uint32Array.from(this, t => t[getTickProp("timestamp")]),
      Float64Array.from(this, t => t[getTickProp("minRtt")]),
      Uint8Array.from(this, t => t[getTickProp("status")])
    ];
  }
}

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

const missingTick = (start, interval, tick, msg = "missing") => [
  tick * interval + start, // timestamp
  tick, // tick
  null, // minRtt
  statusMap.missing, // status
  msg, // statusMsg
  0, // drift
  false // outOfBand
];

// note that msmMetaData also contains probe_jitter, prbId
const reduceValidTicks = msmMetaData => srcArr => {
  // takes
  // msmMetaData: metadata from the RIPE Atlas measurements API
  // (curried) ticksArray: array of ticks in the ticksArrayType
  // items in the array should use as getter: getTickProp(<FIELDNAME>)

  if (!srcArr.length) {
    return [];
  }
  console.log("\n-+-+-+-+-+-+-+-+-");
  process.stdout.write(`[start probe ${msmMetaData.prbId}]`);
  process.stdout.write(`[ ticks in file ${srcArr.length}]`);
  const minRttField = getTickProp("minRtt");
  const tickField = getTickProp("tick");
  const statusMsgField = getTickProp("statusMsg");

  /*
   *                                               ticksNo
   *                      i
   *                      +----------------------------------------------------------+
   *                      |                                                          |
   *                  startTickC                                           startTickC + ticksNo
   *
   *                                              srcArray.length
   *                 j + startTickC                                   j + startTickC + srcArray.length
   *                           +-------ddd---------------------|---------------+
   *                           |      |   |                   | |              |
   *                                   | |                  |    |
   *                                    |                  |     |
   * resultArray          mmmmm+++++++++d+++++++++++++++++++ggggg+++++++++++++++mmmmmmm
   *
   */

  // this is the tick number of the start of the time slice that was requested
  // relative to the start of the measurment.
  const startTickC = Math.round(
    (msmMetaData.seekStartTime - msmMetaData.start) / msmMetaData.interval
  );

  // The array that will be returned eventually.
  let resultArr = [];

  // The number of ticks that we are going to deliver in the result fillAr
  const ticksNo = msmMetaData.exactTicks;

  process.stdout.write(`[source array length: ${srcArr.length}]`);
  process.stdout.write(`[first index: ${startTickC}]`);
  process.stdout.write(`[number of ticks (calculated): ${ticksNo}]`);

  // this is the index of the tick for the srcArr mapping to the
  // current resultArr[i] element
  let j = 0;

  for (let i = startTickC; i < startTickC + ticksNo - 1; i++) {
    let srcTick = srcArr[j];
    let srcTickI = srcTick && srcTick[tickField];
    let nextSrcTick = srcArr[j + 1];
    let nextSrcTickI = nextSrcTick && nextSrcTick[tickField];

    if (!srcTick) {
      // probably the end of the rttArray
      process.stdout.write("!");
      resultArr.push(
        missingTick(msmMetaData.start, msmMetaData.interval, i, "missingEnd")
      );
      continue;
    }

    // double tick, e.g. 1,1
    if (srcTickI === nextSrcTickI) {
      process.stdout.write("d");
      // 1. pick the first run, if it didn't timeout
      // 2. pick the second one if it didn' timeoue
      // 3. pick the first one anyway
      if (Number.isFinite(srcTick[minRttField])) {
        if (statusMsgField) {
          srcTick[statusMsgField] = "doubletick1";
        }
        resultArr.push(srcTick);
        j += 2;
      } else if (Number.isFinite(nextSrcTick[minRttField])) {
        if (statusMsgField) {
          nextSrcTick[statusMsgField] = "doubletick2";
        }
        resultArr.push(nextSrcTick);
        j += 2;
      } else {
        if (statusMsgField) {
          srcTick[statusMsgField] = "doubletick3";
        }
        resultArr.push(srcTick);
        j += 2;
      }
      continue;
    }

    // normal order e.g. 1,2
    // process.stdout.write(`-${srcTickI}->${i}`);
    if (srcTickI === i) {
      if (srcTick[minRttField] < 10) {
        process.stdout.write(rttMap["s"]);
      } else if (srcTick[minRttField] < 50) {
        process.stdout.write(rttMap["m"]);
      } else if (srcTick[minRttField] < 100) {
        process.stdout.write(rttMap["l"]);
      } else if (Number.isFinite(srcTick[minRttField])) {
        process.stdout.write(rttMap["xl"]);
      } else {
        process.stdout.write("x");
      }
      resultArr.push(srcTick);
      j++;

      continue;
    }

    // gap, e.g. 1,3 or 1,4
    if (i < srcTickI) {
      process.stdout.write("g");
      resultArr.push(
        missingTick(msmMetaData.start, msmMetaData.interval, i, "missingGap")
      );
      continue;
    }

    // probably missing at the beginning of the srcArray.
    process.stdout.write(`-${i}->${srcTickI} ${i + 1}->${nextSrcTickI}-`);

    process.stdout.write("m");
    resultArr.push(
      missingTick(msmMetaData.start, msmMetaData.interval, i, "missingFront")
    );
  }

  return new AtlasResults(...resultArr);
};

const composeResultsWithMaxTimeStamp = (transform, reduce) => rowData => {
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
    reduce(r).toOutputArrays(outputFormat),
    (Number.isFinite(minTimeStamp) && minTimeStamp) || null,
    (maxTimeStamp && maxTimeStamp) || null
  ];
};

export const transduceResultsToTicks = msmMetaData => {
  return composeResultsWithMaxTimeStamp(
    transformToTickArray(msmMetaData),
    reduceValidTicks(msmMetaData)
  );
};
