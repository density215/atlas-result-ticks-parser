import fs from "fs";
import errors from "restify-errors";

const rtthmm = require("rtthmm").binding;

import fetch from "node-fetch";
import { loadMsmDetailData } from "@ripe-rnd/ui-datastores";
import { DateTime, Duration } from "luxon";

import {
  validateCommaSeparatedListWithStrings,
  validateCommaSeparatedListWithNumbers,
  validateNumber,
  validateAsASCIIStartingWithLetter,
  validateDateTimeAsISOorTimeStamp,
  validateMaxDuration,
  validateNotInTheFuture,
  validateMsmOrRejectMessages,
  validateHasEnoughTicks
} from "./validators";

import {
  getTicksOutputSchema,
  ticksArrayType,
  transduceToTicksWithMinMaxTimestamps
} from "./transformers";
import { createSummary } from "./formatter";

import { hbaseMsmProbeTimeRangeScan } from "./adapters";

const dateKeyFormat = "yyyy-LL-dd'T'HH:mm";

const validateProbeIds = probeIds =>
  validateCommaSeparatedListWithNumbers(probeIds);
const validatePrbId = prbId => validateNumber(prbId);
const validateStart = start => validateDateTimeAsISOorTimeStamp(start);
const validateStop = stop => validateDateTimeAsISOorTimeStamp(stop);
const validateTicksNo = ticksNo => validateNumber(ticksNo);
const validateType = type => validateAsASCIIStartingWithLetter(type);
const validateFormat = format => validateAsASCIIStartingWithLetter(format);
const validateInclude = includeFields =>
  validateCommaSeparatedListWithStrings(includeFields);
const validateMsmId = msmId => validateNumber(msmId);

const allowedQueryParams = {
  msmId: validateMsmId,
  prbId: validatePrbId,
  probe_ids: validateProbeIds,
  start: validateStart,
  stop: validateStop,
  ticks_no: validateTicksNo,
  type: validateType,
  format: validateFormat,
  include: validateInclude
};

const getSpread = msmMetaData => Math.min(msmMetaData.interval / 2, 400);

const parseAsDt = dt => {
  if (!DateTime.fromISO(dt).invalid) {
    return DateTime.fromISO(dt, { zone: "utc" });
  }
  if (!DateTime.fromSeconds(parseInt(dt)).invalid) {
    return DateTime.fromSeconds(parseInt(dt));
  }
  return false;
};

// parses the start and stop query parms if present
// or fill them out with default values:
// start : 2 weeks ago
// stop  : now
// returns: [ startDt: <DateTime> || false, stopDt: <DateTime> || false]
const parseStartStopDuration = (startDt, stopDt) => {
  const now = DateTime.utc();
  const vStopDt = (stopDt && parseAsDt(stopDt)) || now;
  const vStartDt =
    (startDt && parseAsDt(startDt)) || now.minus({ weeks: 2 }).startOf("day");
  if (vStartDt >= vStopDt) {
    return [false, false];
  }

  return [vStartDt, vStopDt];
};

const validateQueryParams = queryParams => {
  return Object.keys(queryParams).reduce((validatedQueryParams, rqp) => {
    validatedQueryParams[rqp] =
      Object.keys(allowedQueryParams).some(aqp => aqp === rqp) &&
      allowedQueryParams[rqp](queryParams[rqp]);
    return validatedQueryParams;
  }, {});
};

const computeRttHmm = (tsArr, rttArr, statusArr) => {
  let statusMatrix = [];
  try {
    statusMatrix = rtthmm.fit(tsArr, rttArr, statusArr);
  } catch (error) {
    console.log("rtthmm crashed");
    console.log(error);
    statusMatrix = new Array(tsArr.length).fill("E");
  }
  return statusMatrix;
};

const makeResponse = ({
  validationErrors,
  defaultFormat,
  msmId,
  prbId,
  startTime,
  stopTime,
  type,
  res: res,
  next: next,
  ...props
}) => {
  res.setHeader("content-type", "application/json; charset=utf-8");

  // Generic rejected field messages
  if (validationErrors) {
    return next(
      new errors.UnprocessableEntityError(
        "field(s) `%s` cannot be processed: not found or containing invalid keys.",
        validationErrors
      )
    );
  }

  // if a custom errMsg was set, then throw with that one.
  if (props.errMsg) {
    return next(new errors.UnprocessableEntityError(props.errMsg));
  }

  loadMsmDetailData({ msmId, apiServer: "atlas.ripe.net", fetch })
    .then(
      msmMetaData => {
        // last validation (for which we need the metadata)

        const validation = validateMsmOrRejectMessages(msmMetaData);
        if (validation !== true) {
          console.log(validation);
          next(new errors.UnprocessableEntityError(validation.join(" ")));
        }

        const ticksValidation = validateHasEnoughTicks(100)({
          interval: msmMetaData.interval,
          startTime,
          stopTime
        });

        if (ticksValidation !== true) {
          next(new errors.UnprocessableEntityError(ticksValidation));
        }

        // end of validation of metadata

        return hbaseMsmProbeTimeRangeScan({
          msmMetaData: {
            ...msmMetaData,
            msmId: msmId,
            start: msmMetaData.start_time,
            spread: msmMetaData.spread || getSpread(msmMetaData),
            probe_jitter: 3,
            exactTicks: Math.floor(
              stopTime.diff(startTime) / 1000 / msmMetaData.interval
            ),
            seekStartTime: startTime.toSeconds()
          },
          prbId: prbId,
          startTime: startTime.toFormat(dateKeyFormat),
          stopTime: stopTime.toFormat(dateKeyFormat),
          transducer:
            (type === "ticks" && transduceToTicksWithMinMaxTimestamps) || null
        });
      },
      err => {
        console.log(err);
        next(
          new errors.InternalServerError(
            `Could not load metadata for msmId ${msmId}. ${(err &&
              (err.detail || err)) ||
              ""} `
          )
        );
      }
    )
    .catch(err =>
      next(
        new errors.InternalServerError(
          `Could not load metadata for msmId ${msmId}. ${(err &&
            ((err.detail && err.detail) || err)) ||
            ""}`
        )
      )
    )
    .then(
      r => {
        let statusMatrix;
        let csvArr, tsArr, rttArr, statusArr;
        const [
          tickArrs, //[csvArr, tsArr, rttArr, statusArr],
          [minTimeStamp, maxTimeStamp]
        ] = r;
        switch (type) {
          case "raw":
            console.log(`[start hmm for probe ${prbId}]`);
            [csvArr, tsArr, rttArr, statusArr] = tickArrs;
            statusMatrix = computeRttHmm(tsArr, rttArr, statusArr);
            res.send(200, {
              results: Array.from(csvArr, (s, i) => [
                ...s,
                tsArr[i],
                rttArr[i],
                statusArr[i],
                statusMatrix[i]
              ]),
              // enumeration of the above.
              // TODO: make this more constistant on the transduces
              // (as a last chained function, like .toSchemaOutputArray or so)
              metadata: {
                schema: [
                  ...getTicksOutputSchema,
                  "timestamp",
                  "rtt",
                  "status (double)",
                  "state"
                ],
                distribution: {
                  package_version: __PACKAGE_VERSION__,
                  build: __BUILD__
                }
              },
              ticksNo: rttArr.length,
              seekStartTime: startTime,
              minTimeStamp:
                (minTimeStamp &&
                  DateTime.fromSeconds(minTimeStamp)
                    .toUTC()
                    .toISO()) ||
                null,
              seekStopTime: stopTime,
              maxTimeStamp:
                (maxTimeStamp &&
                  DateTime.fromSeconds(maxTimeStamp)
                    .toUTC()
                    .toISO()) ||
                null
            });
            break;

          case "summary":
            console.log(`[start hmm for probe ${prbId}]`);
            // let [
            //   [csvArr, tsArr, rttArr, statusArr],
            //   [minTimeStamp, maxTimeStamp]
            // ] = r;
            [csvArr, tsArr, rttArr, statusArr] = tickArrs;
            statusMatrix = computeRttHmm(tsArr, rttArr, statusArr);
            const summary = createSummary({
              stateseq: statusMatrix,
              timestamps: tsArr,
              rtt: rttArr,
              minTimeStamp,
              maxTimeStamp
            });
            res.send(200, {
              ...summary,
              ticksNo: rttArr.length,
              lastMinRtt: rttArr[rttArr.length - 1],
              seekStartTime: startTime,
              minTimeStamp:
                (minTimeStamp &&
                  DateTime.fromSeconds(minTimeStamp)
                    .toUTC()
                    .toISO()) ||
                null,
              seekStopTime: stopTime,
              maxTimeStamp:
                (maxTimeStamp &&
                  DateTime.fromSeconds(maxTimeStamp)
                    .toUTC()
                    .toISO()) ||
                null,
              metadata: {
                distribution: {
                  package_version: __PACKAGE_VERSION__,
                  build: __BUILD__,
                  environment: __ENVIRONMENT__
                }
              }
            });
            break;

          case "ticks":
            console.log(r);
            // let [csvArr, [minTimeStamp, maxTimeStamp]]
            res.send(200, {
              results: tickArrs,
              // enumeration of the above.
              // TODO: make this more constistant on the transduces
              // (as a last chained function, like .toSchemaOutputArray or so)
              metadata: {
                schema: ticksArrayType,
                distribution: {
                  package_version: __PACKAGE_VERSION__,
                  build: __BUILD__,
                  environment: __ENVIRONMENT__
                }
              },
              ticksNo: tickArrs.length,
              seekStartTime: startTime,
              minTimeStamp:
                (minTimeStamp &&
                  DateTime.fromSeconds(minTimeStamp)
                    .toUTC()
                    .toISO()) ||
                null,
              seekStopTime: stopTime,
              maxTimeStamp:
                (maxTimeStamp &&
                  DateTime.fromSeconds(maxTimeStamp)
                    .toUTC()
                    .toISO()) ||
                null
            });
        }
        return next();
      },
      err => next(new errors.InternalServerError(err))
    )
    .catch(err => next(new errors.InternalServerError(err)));
};

export const msmTrendsForProbe = ({ type }) => (req, res, next) => {
  let validationErrors = [];
  let startTime = null;
  let stopTime = null;
  let errMsg = null;

  const validation = validateQueryParams({
    ...req.query,
    msmId: req.params.msmId,
    prbId: req.params.prbId
  });

  validationErrors =
    (Object.values(validation).some(v => v === false) &&
      Object.entries(validation)
        .filter(v => v[1] === false)
        .map(e => e[0])) ||
    [];

  if (validationErrors.length === 0) {
    [startTime, stopTime] = parseStartStopDuration(
      req.query.start,
      req.query.stop
    );

    if (!startTime) {
      validationErrors.push("start");
    }
    if (!stopTime) {
      validationErrors.push("stop");
    }

    errMsg =
      (!validateMaxDuration(Duration.fromObject({ month: 1 }))(
        startTime,
        stopTime
      ) &&
        "Sorry, cannot parse durations longer than one month.") ||
      null;

    errMsg =
      (!validateNotInTheFuture(startTime) &&
        "Sorry, cannot parse datetimes in the future") ||
      errMsg;

    // only validate stopTime if the user set the queryParam
    // otherwise you'll see checks against the time jitter of JS & NodeJS
    errMsg =
      (req.query.stop &&
        !validateNotInTheFuture(stopTime) &&
        "Sorry, cannot parse datetimes in the future") ||
      errMsg;
  }

  // more validation lives in the promise that returns the msmMetaData.

  makeResponse({
    type: type,
    validationErrors: (validationErrors.length > 0 && validationErrors) || null,
    errMsg: (errMsg && errMsg) || null,
    defaultFormat: "json",
    msmId: req.params.msmId,
    prbId: req.params.prbId,
    startTime: startTime,
    stopTime: stopTime,
    res: res,
    next: next
  });
};

export const msmTicksForProbe = (req, res, next) => {
  let validationErrors = [];
  let startTime = null;
  let stopTime = null;
  let errMsg = null;

  const validation = validateQueryParams({
    ...req.query,
    msmId: req.params.msmId,
    prbId: req.params.prbId
  });

  validationErrors =
    (Object.values(validation).some(v => v === false) &&
      Object.entries(validation)
        .filter(v => v[1] === false)
        .map(e => e[0])) ||
    [];

  if (validationErrors.length === 0) {
    [startTime, stopTime] = parseStartStopDuration(
      req.query.start,
      req.query.stop
    );

    if (!startTime) {
      validationErrors.push("start");
    }
    if (!stopTime) {
      validationErrors.push("stop");
    }

    errMsg =
      (!validateMaxDuration(Duration.fromObject({ month: 1 }))(
        startTime,
        stopTime
      ) &&
        "Sorry, cannot parse durations longer than one month.") ||
      null;

    errMsg =
      (!validateNotInTheFuture(startTime) &&
        "Sorry, cannot parse datetimes in the future") ||
      errMsg;

    // only validate stopTime if the user set the queryParam
    // otherwise you'll see checks against the time jitter of JS & NodeJS
    errMsg =
      (req.query.stop &&
        !validateNotInTheFuture(stopTime) &&
        "Sorry, cannot parse datetimes in the future") ||
      errMsg;
  }

  makeResponse({
    type: "ticks",
    validationErrors: (validationErrors.length > 0 && validationErrors) || null,
    errMsg: (errMsg && errMsg) || null,
    defaultFormat: "json",
    msmId: req.params.msmId,
    prbId: req.params.prbId,
    startTime: startTime,
    stopTime: stopTime,
    res: res,
    next: next
  });
};
