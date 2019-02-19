import { DateTime } from "luxon";

export const validateCommaSeparatedListWithNumbers = list =>
  list.split(",").every(n => validateNumber(n));

export const validateCommaSeparatedListWithStrings = list =>
  list.split(",").every(s => validateAsASCIIStartingWithLetter(s));

export const validateNumber = n => {
  const vN = parseInt(n);
  return Number.isFinite(vN) && vN;
};

export const validateProbeIds = probeIds =>
  probeIds.split(",").every(n => {
    const prbId = parseInt(n);
    return Number.isFinite(prbId) && prbId;
  });

export const validateAsASCIIStartingWithLetter = string =>
  string.match(/([a-zA-Z][a-zA-Z0-9]+)/)[0] === string;

export const validateDateTimeAsISOorTimeStamp = datetime =>
  !DateTime.fromISO(datetime).invalid ||
  !DateTime.fromSeconds(parseInt(datetime)).invalid;

export const validateMsmOrRejectMessages = msmMetaData => {
  let validateRejection = [];
  // we need at least 100 ticks
  const minDuration = 100 * msmMetaData.interval;
  if (msmMetaData.is_oneoff) {
    validateRejection.push(
      "Not a recurring measuremnent. Trends are only available for recurring measurements."
    );
  }
  if (msmMetaData.type !== "ping") {
    validateRejection.push(
      "Not a ping measurement. Trends are only available for ping measurements."
    );
  }
  if (
    !msmMetaData.is_oneoff &&
    DateTime.utc().minus({ seconds: minDuration }) <=
      DateTime.fromSeconds(parseInt(msmMetaData.start_time))
  ) {
    validateRejection.push(
      "Not enough measurement results to calculate trends. Trends need at least 100 non-empty measurement results ('ticks')."
    );
  }
  return (validateRejection.length === 0 && true) || validateRejection;
};
