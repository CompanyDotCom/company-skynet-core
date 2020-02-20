import { handler as fH } from './handlers/fetch';
import { handler as dTH } from './handlers/directTransition';
import { handler as bTH } from './handlers/bulkTransition';
import { handler as sDb } from './handlers/setupDatabase';

/**
 * This is the fetch request handler
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param { throttleLmts: object, safeThrottleLimit: int, reserveCapForDirect: int, retryCntForCapacity: int } are the throttle limits
 * // CR: Mickey: I think safeThrottleLimit and reserveCapForDirect are nums and not ints
 * @param {string} r is the region of AWS that this service is running in
 * @param {string} s service is the name of the service
 * @param {string} a account is AWS the account number
 * @param {object} b is the event input
 * @param {function} c is the worker function that has the business logic to process the request
 */
export const fetchHandler = async (
  AWS,
  {
    throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
    // eslint-disable-next-line arrow-body-style
  }, r, s, a, b, c) => {
  return fH(
    AWS,
    {
      throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
    }, r, s, a, b, c,
  );
};

/**
 * This is the direct transition request handler
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param { throttleLmts: object, safeThrottleLimit: int, reserveCapForDirect: int, retryCntForCapacity: int } are the throttle limits
 * @param {string} r is the region of AWS that this service is running in
 * @param {string} s service is the name of the service
 * @param {string} a account is AWS the account number
 * @param {object} b is the event input
 * @param {function} c is the worker function that has the business logic to process the request
 */
export const directTransitionHandler = async (
  AWS,
  {
    throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
    // eslint-disable-next-line arrow-body-style
  }, r, s, a, b, c) => {
  return dTH(
    AWS,
    {
      throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
    }, r, s, a, b, c,
  );
};

/**
 * This is the bulk transition request handler
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param { throttleLmts: object, safeThrottleLimit: int, reserveCapForDirect: int, retryCntForCapacity: int } are the throttle limits
 * @param {string} r is the region of AWS that this service is running in
 * @param {string} s service is the name of the service
 * @param {string} a account is AWS the account number
 * @param {object} b is the event input
 * @param {function} c is the worker function that has the business logic to process the request
 */
export const bulkTransitionHandler = async (
  AWS,
  {
    throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
    // eslint-disable-next-line arrow-body-style
  }, r, s, a, b, c) => {
  return bTH(
    AWS,
    {
      throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity,
    }, r, s, a, b, c,
  );
};

/**
 * This is the fetch request handler
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param {Object} d is the data to be saved
 * @param {string} s service is the name of the service
 */
export const setupDatabase = async (AWS, d, s) => {
  let data = '';
  if (typeof d === 'object') {
    data = d;
  } else {
    try {
      data = JSON.parse(d);
    } catch (e) {
      console.log('Unable to parse the database file. Please check if it is a valid JSON document.');
      return;
    }
  }
  // eslint-disable-next-line consistent-return
  return sDb(AWS, data, s);
};
