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
const createOutputArray = value => [value[1], value[3], value[4]];

// The public 'interface'
export const getTickProp = fieldName => {
  const iName = ticksArrayType.indexOf(fieldName);
  return iName !== -1 ? iName : null;
};
export const getTicksOutputSchema = createOutputArray(ticksArrayType);

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

  // this is the tick number of the start of the time slice that was requested
  // relative to the start of the measurment.
  const firstSeenTickC = srcArr[0][tickField];
  const startTickC = Math.round(
    (msmMetaData.seekStartTime - msmMetaData.start) / msmMetaData.interval
  );

  let resultArr = [];
  // This is the counter that keeps track of the diff between
  // the result resultArr array and the counter in the source sourceArr
  let diffC = 0;
  // The number of ticks that we are going to deliver in the result fillAr
  const numberOfTicks = msmMetaData.exactTicks;

  // process.stdout.write(`[first index: ${offsetStart}]`);
  process.stdout.write(`[last Index: ${numberOfTicks - 1}]`);
  process.stdout.write(`[number of ticks (calculated): ${numberOfTicks}]`);
  // let timeStampsBuf = new ArrayBuffer(numberOfTicks * 4);
  // let timeStampsArr = new Uint32Array(timeStampsBuf);
  // let rttBuf = new ArrayBuffer(numberOfTicks * 8);
  // let rttArr = new Float64Array(rttBuf);
  // let statusBuf = new ArrayBuffer(numberOfTicks);
  // let statusArr = new Uint8Array(statusBuf);

  for (let i = 0; i < numberOfTicks - 1; i++) {
    let tickC = i + diffC;
    let srcTick = srcArr[tickC];

    if (!srcTick) {
      // probably the end of the rttArray
      process.stdout.write("!");
      // process.stdout.write(`[no ${i}]`);
      continue;
    }

    let srcTickC = srcTick[tickField];

    // write the resulting tick number with the
    // start offset in the
    // tick that's ready to be pushed to the result array.
    const newTickC = tickC + firstSeenTickC;

    // sourceTick[getTickProp("tick")] = sourceTick[getTickProp("tick")] + ci;

    let nextTick = srcArr[tickC + 1];
    let nextTickC = nextTick && nextTick[tickField];
    // let iOff = i + ci + offsetStart;

    // normal order e.g. 1,2
    if (newTickC === srcTickC && srcTickC + 1 === nextTickC) {
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
      // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
      //   rta[getTickProp("timestamp")],
      //   rta[getTickProp("minRtt")],
      //   rta[getTickProp("status")]
      // ];
      continue;
    }

    // double tick, e.g. 1,1
    if (newTickC === srcTickC && srcTickC === nextTickC) {
      process.stdout.write("d");
      // 1. pick the first run, if it didn't timeout
      // 2. pick the second one if it didn' timeoue
      // 3. pick the first one anyway
      if (Number.isFinite(srcTick[minRttField])) {
        if (statusMsgField) {
          srcTick[statusMsgField] = "doubletick1";
        }
        resultArr.push(srcTick);
        // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
        //   rta[getTickProp("timestamp")],
        //   rta[getTickProp("minRtt")],
        //   rta[getTickProp("status")]
        // ];
        diffC--;
        // iOff--;
      } else if (Number.isFinite(nextTick[minRttField])) {
        if (statusMsgField) {
          nextTick[statusMsgField] = "doubletick2";
        }
        resultArr.push(nextTick);
        // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
        //   nextTick[getTickProp("timestamp")],
        //   nextTick[getTickProp("minRtt")],
        //   nextTick[getTickProp("status")]
        // ];
        diffC--;
        // iOff--;
      } else {
        if (statusMsgField) {
          srcTick[statusMsgField] = "doubletick3";
        }
        resultArr.push(srcTick);
        // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
        //   rta[getTickProp("timestamp")],
        //   rta[getTickProp("minRtt")],
        //   rta[getTickProp("status")]
        // ];
        diffC--;
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
    // note that this ONLY fills gaps and does not left or right pads
    if (newTickC + 1 < nextTickC) {
      // cycle until we reach the next tick
      let aiTs, lastAiTs;
      for (let ni = 0; ni < nextTickC - tickC; ni++) {
        // console.log(ni+ci+i);
        // end of the rttArray
        if (!nextTick) {
          continue;
        }

        process.stdout.write("m");
        lastAiTs = (aiTs && aiTs) || srcTick[getTickProp("timestamp")];
        aiTs = srcTick[getTickProp("timestamp")] + msmMetaData.interval * ni;
        //nextTick[outputMap.indexOf("drift")];
        resultArr.push([
          aiTs, // timeStamp
          ni + tickC + firstSeenTickC, // tick
          null, // minRtt
          statusMap.missing, // status
          "missing", // statusMsg
          aiTs - lastAiTs, // drift
          false // outOfBand
        ]);
        // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
        //   aiTs,
        //   0,
        //   statusMap.missing
        // ];
        diffC++;
        // iOff++;
      }
      diffC--;
      // iOff--;
      continue;
    }

    process.stdout.write(`l${newTickC}->${srcTickC}-`);
    if (statusMsgField) {
      srcTick[statusMsgField] = "leftover";
    }
    resultArr.push(srcTick);
    // [timeStampsArr[iOff], rttArr[iOff], statusArr[iOff]] = [
    //   rta[getTickProp("timestamp")],
    //   rta[getTickProp("minRtt")],
    //   rta[getTickProp("status")]
    // ];
    // ci++;
    // iOff++;
  }
  // console.log(rttArr);

  // if the ticksArray length and the number of ticks as calculated from the metadata (start_time + n * interval + spread)
  // still do not match up at this point, then that could only have happened
  // at the beginning of the array.
  // All other gaps should be filled by the above conditional fills of the array.
  // So we go over the array once more to left pad the array.
  console.log(resultArr.length);
  console.log(msmMetaData.exactTicks);
  console.log(resultArr[0][getTickProp("timestamp")] * 1000);
  console.log(
    msmMetaData.seekStartTime +
      msmMetaData.interval * 1000 +
      msmMetaData.spread * 1000
  );

  const offsetStart = firstSeenTickC - startTickC;
  if (offsetStart > 0) {
    console.log(`[${offsetStart} offset boogie]`);
    console.log(diffC);

    resultArr = [...Array(offsetStart)]
      .map((_, i) => {
        const tick = i + diffC + startTickC;
        const ts = i * msmMetaData.interval + msmMetaData.seekStartTime;
        return [
          // msmMetaData.start + msmMetaData.interval * i + msmMetaData.spread, // fictional, calculated timeStamp
          ts,
          tick, // tick
          null, // minRtt
          statusMap.missing, // status
          "missing", // statusMsg
          0, // drift
          false // outOfBand
        ];
      })
      .concat(resultArr);
    // numberOfTicks = msmMetaData.exactTicks;
  }

  // let timeStampsBuf = new ArrayBuffer(numberOfTicks * 4);
  // let timeStampsArr = new Uint32Array(timeStampsBuf);
  // let rttBuf = new ArrayBuffer(numberOfTicks * 8);
  // let rttArr = new Float64Array(rttBuf);
  // let statusBuf = new ArrayBuffer(numberOfTicks);
  // let statusArr = new Uint8Array(statusBuf);

  return [
    resultArr.map(createOutputArray),
    Uint32Array.from(resultArr, t => t[getTickProp("timestamp")]),
    // timeStampsArr
    //   .subarray(0, fillAr.length)
    //   .copyWithin(offsetStart - 1, 0, fillAr.length)
    //   .fill(0, 0, offsetStart),
    Float64Array.from(resultArr, t => t[minRttField]),
    // rttArr
    //   .subarray(0, fillAr.length)
    //   .copyWithin(offsetStart - 1, 0, fillAr.length)
    //   .fill(0, 0, offsetStart),
    Uint8Array.from(resultArr, t => t[getTickProp("status")])
    // statusArr
    //   .subarray(0, fillAr.length)
    //   .copyWithin(offsetStart - 1, 0, fillAr.length)
    //   .fill(0, 0, offsetStart)
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

export const transduceResultsToTicks = msmMetaData => {
  return composeRowsWithMaxTimeStamp(
    transformToTickArray(msmMetaData),
    reduceValidTicks(msmMetaData)
  );
};
