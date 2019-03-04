import fs from "fs";
import errors from "restify-errors";

const rtthmm = require("rtthmm").binding;

import fetch from "node-fetch";
import { loadMsmDetailData } from "@ripe-rnd/ui-datastores";
import { DateTime } from "luxon";

import {
  validateCommaSeparatedListWithStrings,
  validateCommaSeparatedListWithNumbers,
  validateNumber,
  validateAsASCIIStartingWithLetter,
  validateDateTimeAsISOorTimeStamp,
  validateMsmOrRejectMessages
} from "./validators";

import { createSummary } from "./formatter";

const { hbaseMsmProbeTimeRangeScan } = require("./adapters");

const dateKeyFormat = "yyyy-LL-dd'T'";

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

// let VERSION, BUILD;
// try {
//   VERSION = fs.readFileSync("./PACKAGE_VERSION.txt", "utf8");
// } catch (err) {
//   if (err.code === "ENOENT") {
//     VERSION = JSON.parse(fs.readFileSync("./package.json", "utf8")).version;
//   } else {
//     throw "Cannot find either VERSION.txt or package.json. Cannot continue";
//   }
// }
// console.log("version :\t" + VERSION);

// try {
//   BUILD = fs.readFileSync("./BUILD.txt", "utf8");
// } catch (err) {
//   if (err.code === "ENOENT") {
//     BUILD = "not-built-dev";
//   } else {
//     throw "Something wrong with BUILD information. Cannot continue";
//   }
// }
// console.log("build :\t" + BUILD);

const validateQueryParams = queryParams => {
  return Object.keys(queryParams).reduce((validatedQueryParams, rqp) => {
    validatedQueryParams[rqp] =
      Object.keys(allowedQueryParams).some(aqp => aqp === rqp) &&
      allowedQueryParams[rqp](queryParams[rqp]);
    return validatedQueryParams;
  }, {});
};

const makeResponse = ({
  validationErrors,
  defaultFormat,
  msmId,
  prbId,
  type,
  res: res,
  next: next,
  ...props
}) => {
  res.setHeader("content-type", "application/json; charset=utf-8");
  if (validationErrors) {
    return next(
      new errors.UnprocessableEntityError(
        "field(s) `%s` cannot be processed: not found or containing invalid keys.",
        validationErrors
      )
    );
  }

  loadMsmDetailData({ msmId, apiServer: "atlas.ripe.net", fetch })
    .then(
      msmMetaData => {
        /* stupid hardcoded stuff for now */
        const now = DateTime.utc();
        const stopTime = now.toFormat(dateKeyFormat);
        const startTime = now.minus({ weeks: 2 }).startOf("day");

        return hbaseMsmProbeTimeRangeScan({
          msmMetaData: {
            ...msmMetaData,
            msmId: msmId,
            start: msmMetaData.start_time,
            spread: msmMetaData.spread || getSpread(msmMetaData),
            probe_jitter: 3,
            exactTicks: Math.floor(
              now.diff(startTime) / 1000 / msmMetaData.interval
            )
          },
          prbId: prbId,
          startTime,
          stopTime
        });
      },
      err => {
        next(
          new errors.InternalServerError(
            `Could not load metadata for msmId ${msmId}.\n${err.detail}`
          )
        );
      }
    )
    .catch(err =>
      next(
        new errors.InternalServerError(
          `Could not load metadata for msmId ${msmId}\n${err.detail}`
        )
      )
    )
    .then(
      ([[csvArr, tsArr, rttArr, statusArr], minTimeStamp, maxTimeStamp]) => {
        let statusMatrix = [];
        console.log(`[start hmm for probe ${prbId}]`);
        try {
          statusMatrix = rtthmm.fit(tsArr, rttArr, statusArr);
        } catch (error) {
          console.log("rtthmm crashed");
          console.log(error);
          statusMatrix = new Array(csvArr.length).fill("E");
        }

        if (type === "raw") {
          res.send(200, {
            results: Array.from(csvArr, (s, i) => [
              ...s,
              tsArr[i],
              rttArr[i],
              statusArr[i],
              statusMatrix[i]
            ]),
            minTimeStamp:
              (minTimeStamp &&
                DateTime.fromSeconds(minTimeStamp)
                  .toUTC()
                  .toISO()) ||
              null,
            maxTimeStamp:
              (maxTimeStamp &&
                DateTime.fromSeconds(maxTimeStamp)
                  .toUTC()
                  .toISO()) ||
              null
          });
        }

        if (type === "summary") {
          const summary = createSummary({
            stateseq: statusMatrix,
            timestamps: tsArr,
            rtt: rttArr,
            minTimeStamp,
            maxTimeStamp
          });
          res.send(200, {
            ...summary,
            minTimeStamp:
              (minTimeStamp &&
                DateTime.fromSeconds(minTimeStamp)
                  .toUTC()
                  .toISO()) ||
              null,
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
  console.log(req.query);
  const validation = validateQueryParams({
    ...req.query,
    msmId: req.params.msmId,
    prbId: req.params.prbId
  });
  const validationErrors =
    (Object.values(validation).some(v => v === false) &&
      Object.entries(validation)
        .filter(v => v[1] === false)
        .map(e => e[0])) ||
    null;
  makeResponse({
    type: type,
    validationErrors: validationErrors,
    defaultFormat: "json",
    msmId: req.params.msmId,
    prbId: req.params.prbId,
    res: res,
    next: next
  });
};
