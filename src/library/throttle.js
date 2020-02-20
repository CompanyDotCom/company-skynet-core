import { fetchRecordsByQuery, incrementColumn } from './dynamo';
import { sleep } from './util';

/**
 * Fetches and returns the number of calls made to the service for the given
 * day, hour, minute and second
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param {String} serviceName is the service for which the metrics are to be fetched
 * @returns {{ second: Number, minute: Number, hour: Number, day: Number }}
 */
export const getCallsMade = async (AWS, serviceName) => {
  const currMs = Date.now();
  // Get the next second (without the milliseconds)
  const currSec = Math.floor(currMs / 1000) + 1;
  // Get the next minute at its second 0
  const currMin = currSec - (currSec % 60) + 60;
  // Get the next hour at its 0th minute and 0th second
  const currHr = currMin - (currMin % (60 * 60)) + (60 * 60);
  // Get the next day at 00:00:00
  const currDay = currHr - (currHr % (24 * 60 * 60)) + (24 * 60 * 60);

  // Construct the common properties for the next queries
  const queryObj = {
    TableName: 'apiCallCount',
    ExpressionAttributeNames: { '#pk': 'serviceAndDuration', '#et': 'expiryTime' },
    KeyConditionExpression: '#pk = :sd AND #et = :et',
  };

  // Generate a list of promises to get the records for current second, minute,
  // hour and day
  const proms = [
    fetchRecordsByQuery(
      AWS,
      {
        ...queryObj,
        ExpressionAttributeValues: {
          ':sd': { S: `${serviceName}-second` },
          ':et': { N: currSec.toString() },
        },
      },
    ),
    fetchRecordsByQuery(
      AWS,
      {
        ...queryObj,
        ExpressionAttributeValues: {
          ':sd': { S: `${serviceName}-minute` },
          ':et': { N: currMin.toString() },
        },
      },
    ),
    fetchRecordsByQuery(
      AWS,
      {
        ...queryObj,
        ExpressionAttributeValues: {
          ':sd': { S: `${serviceName}-hour` },
          ':et': { N: currHr.toString() },
        },
      },
    ),
    fetchRecordsByQuery(
      AWS,
      {
        ...queryObj,
        ExpressionAttributeValues: {
          ':sd': { S: `${serviceName}-day` },
          ':et': { N: currDay.toString() },
        },
      },
    ),
  ];

  // Wait for the promises to complete
  const promRes = await Promise.all(proms);

  // Return the call counts from the promise results
  return {
    second: promRes[0].callCount ? promRes[0].callCount : 0,
    minute: promRes[1].callCount ? promRes[1].callCount : 0,
    hour: promRes[2].callCount ? promRes[2].callCount : 0,
    day: promRes[3].callCount ? promRes[3].callCount : 0,

  };
};

/**
 * Gets the count of calls that can be made this second without hitting the
 * throttle limit
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param {String} serviceName is the name of the service for which the count is required
 * @param {Boolean} bulk tells whether the call is going to be bulk or direct
 * @returns {Number}
 */
// CR: Mickey: I recommend everyone installing a spellchecker in your editor to avoid
//    accidentally misnaming variables during declaration and then perpetuating
//    the misspelling throughout the codebase via the autocomplete.
//    I use "Code Spell Checker" by StreetSide Software on vsCode
export const getAvailableCallsThisSec = async (
  AWS,
  {
    throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
  },
  serviceName, bulk = true, iter = 0) => {
  if (iter > retryCntForCapacity) {
    return 0;
  }

  if (iter > 0) {
    await sleep(1000);
  }
  const throtLmts = JSON.parse(throttleLmts);
  if (typeof throtLmts.day === 'undefined'
    && typeof throtLmts.hour === 'undefined'
    && typeof throtLmts.minute === 'undefined'
    && typeof throtLmts.second === 'undefined') {
    return 1000000;
  }

  // CR: Mickey: looking at the below makes my head hurt, but I trust you on it and
  //    I can't think of a way that would make it look better

  // CR: Mickey: what is resFact short for? reservedFactor?
  const resFact = bulk === true
    ? (1 - reserveCapForDirect) * safeThrottleLimit
    : 1 * safeThrottleLimit;
  const callsMade = await getCallsMade(AWS, serviceName);
  let availLmt = 'x';
  if (typeof throtLmts.day !== 'undefined'
    && (Math.floor((throtLmts.day * resFact) - callsMade.day) < availLmt
      || availLmt === 'x')) {
    availLmt = Math.floor((throtLmts.day - callsMade.day) * resFact);
  }

  if (typeof throtLmts.hour !== 'undefined'
    && (Math.floor((throtLmts.hour * resFact) - callsMade.hour) < availLmt
      || availLmt === 'x')) {
    availLmt = Math.floor((throtLmts.hour - callsMade.hour) * resFact);
  }

  if (typeof throtLmts.minute !== 'undefined'
    && (Math.floor((throtLmts.minute * resFact) - callsMade.minute) < availLmt
      || availLmt === 'x')) {
    availLmt = Math.floor((throtLmts.minute - callsMade.minute) * resFact);
  }

  if (typeof throtLmts.second !== 'undefined'
    && (Math.floor((throtLmts.second * resFact) - callsMade.second) < availLmt
      || availLmt === 'x')) {
    availLmt = Math.floor((throtLmts.second - callsMade.second) * resFact);
  }

  return availLmt > 0
    ? availLmt : getAvailableCallsThisSec(
      AWS,
      {
        throttleLmts,
        safeThrottleLimit,
        reserveCapForDirect,
        retryCntForCapacity,
      }, serviceName, bulk, iter + 1,
    );
};

// CR: Mickey: the incVal seems to generally be passed as the number of messages handled
//    have we considered handlers that contact the vendor api multiple times?  Or
//    cases of vendors with multiple apis with separate throttle limits?
//    e.g. Yext - scan API with very low limit; knowledge graph API with reasonable limit but some fetch calls consuming 4 or 5 endpoints
//    a Yext activation takes I think 3 calls to create the multiple entities that are needed

/**
 * Increments the per second, minute, hour and day calls made count to the
 * given increment count which is 1 by default
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param {String} serviceName is the name of the service
 * @param {Number} incVal is the increment value. Defaults to 1
 * @returns {Boolean}
 */
export const incrementUsedCount = async (AWS, serviceName, incVal = 1) => {
  const currMs = Date.now();
  const currSec = Math.floor(currMs / 1000) + 1;
  const currMin = currSec - (currSec % 60) + 60;
  const currHr = currMin - (currMin % (60 * 60)) + (60 * 60);
  const currDay = currHr - (currHr % (24 * 60 * 60)) + (24 * 60 * 60);

  const proms = [
    incrementColumn(
      AWS,
      'apiCallCount',
      {
        serviceAndDuration: `${serviceName}-second`,
        expiryTime: currSec,
      },
      'callCount',
      incVal,
    ),
    incrementColumn(
      AWS,
      'apiCallCount',
      {
        serviceAndDuration: `${serviceName}-minute`,
        expiryTime: currMin,
      },
      'callCount',
      incVal,
    ),
    incrementColumn(
      AWS,
      'apiCallCount',
      {
        serviceAndDuration: `${serviceName}-hour`,
        expiryTime: currHr,
      },
      'callCount',
      incVal,
    ),
    incrementColumn(
      AWS,
      'apiCallCount',
      {
        serviceAndDuration: `${serviceName}-day`,
        expiryTime: currDay,
      },
      'callCount',
      incVal,
    ),
  ];
  await Promise.all(proms);
  return true;
};
