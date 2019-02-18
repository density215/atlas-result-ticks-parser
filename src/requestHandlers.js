import fs from "fs";

import errors from "restify";

const validateProbeIds = probeIds =>
  probeIds.split(",").map(n => {
    const prbId = parseInt(n);
    return (Number.isFinite(prbId) && prbId) || false;
  });
const validateStart = () => true;
const validateStop = () => true;
const validateTicksNo = () => true;
const validateType = () => true;
const validateFormat = () => true;
const validateInclude = () => true;

const allowedQueryParams = {
  probe_ids: validateProbeIds,
  start: validateStart,
  stop: validateStop,
  ticks_no: validateTicksNo,
  type: validateType,
  format: validateFormat,
  include: validateInclude
};

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
  warning,
  defaultFormat,
  msmId,
  res: res,
  next: next,
  ...props
}) => {
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.send(200, { warning: warning });

  return next();
};

export const msmTrends = (req, res, next) => {
  console.log(req.query);
  makeResponse({
    warning: validateQueryParams(req.query),
    defaultFormat: "json",
    msmId: req.params.msmId,
    res: res,
    next: next
  });
};
