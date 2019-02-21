import fs from "fs";
import errors from "restify-errors";

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

let VERSION, BUILD;
try {
  VERSION = fs.readFileSync("./PACKAGE_VERSION.txt", "utf8");
} catch (err) {
  if (err.code === "ENOENT") {
    VERSION = JSON.parse(fs.readFileSync("./package.json", "utf8")).version;
  } else {
    throw "Cannot find either VERSION.txt or package.json. Cannot continue";
  }
}
console.log("version :\t" + VERSION);

try {
  BUILD = fs.readFileSync("./BUILD.txt", "utf8");
} catch (err) {
  if (err.code === "ENOENT") {
    BUILD = "not-built-dev";
  } else {
    throw "Something wrong with BUILD information. Cannot continue";
  }
}
console.log("build :\t" + BUILD);

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
          `Could not load metadata for msmId ${msmId}\n${err}`
        )
      )
    )
    .then(
      ([[csvArr, tsArr, rttArr, statusArr], minTimeStamp, maxTimeStamp]) => {
        //   res.send({
        //     metadata: {
        //       ...msmMetaData,
        //       spread: msmMetaData.spread || getSpread(msmMetaData)
        //     },
        //     validate: validateMsmOrRejectMessages(msmMetaData),
        //     start: DateTime.fromSeconds(parseInt(msmMetaData.start_time))
        //       .toUTC()
        //       .toISO()
        //   });
        res.send(200, {
          results: csvArr,
          minTimeStamp: minTimeStamp,
          maxTimeStamp: maxTimeStamp
        });
        return next();
      },
      err => next(new InternalServerError(err))
    )
    .catch(err => next(new InternalServerError(err)));
};

export const msmTrendsForProbe = (req, res, next) => {
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
    validationErrors: validationErrors,
    defaultFormat: "json",
    msmId: req.params.msmId,
    prbId: req.params.prbId,
    res: res,
    next: next
  });
};