/**
 * @description Attempt to JSON.parse input value. If parse fails, return original value.
 * @param {any} v
 * @returns {any}
 */
export const parseJson = v => {
  try {
    return JSON.parse(v);
  } catch (err) {
    return v;
  }
};

/**
 * Returns the string equivalent meaning for given HTTP status code
 * @param {Number} code
 */
const getCodeStatus = code => {
  switch (code) {
    case 200: return 'OK';
    case 201: return 'Created';
    case 400: return 'Bad Request';
    case 500: return 'Internal Server Error';
    default: return undefined;
  }
};

/**
 * @typedef {Object} LambdaProxyIntegrationResponse
 * @property {number} statusCode
 * @property {string} body
 */

/**
 * @description Format HTTP lambda's input, result, and response code to be compliant with Lambda proxy integration
 * @param {number} code
 * @param {*} input
 * @param {*} result
 * @returns {LambdaProxyIntegrationResponse}
 */
export const formatHttpResponse = (code, input, result) => {
  const status = getCodeStatus(code);
  const resp = `HTTP Resp: ${code}${status ? (` - ${status}`) : ''}`;
  let resultObj = {};
  if (result instanceof Error) {
    resultObj.error = result.toString();
  } else if (typeof result === 'object') {
    resultObj = result;
  } else {
    resultObj.message = result;
  }
  return {
    statusCode: code,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true,
    },
    body: JSON.stringify({
      resp,
      input,
      resultObj,
    }),
  };
};

/**
 * Returns the string representation of a given object, error or string
 * @param {Object/ String/ Error} e
 * @returns {String}
 */
export const getErrorString = e => {
  if (e instanceof Error) {
    return e.toString();
  }
  if (typeof e === 'object') {
    return JSON.stringify(e, null, 4);
  }
  return e;
};

/**
 * This is a higher order function to execute the given messageHandler, catch
 * errors and return a proper response
 * @param {any} params
 * @param {Function} messageHandler
 * @returns {Object}
 */
export const neverThrowError = async (params, messageHandler) => {
  const result = {
    status: 'pass',
    params,
  };
  try {
    // CR: Mickey: I see here that procRes.status will always exist and is not set by the handler
    //    can we come up with a better name for the property assigned on the next line so that
    //    in process.js this is not named basically processResponse.processResponse with different
    //    abbreviations?  Maybe result.handlerResponse?
    result.processResp = await messageHandler(params);
  } catch (e) {
    result.status = 'fail';
    result.error = getErrorString(e);
  }
  return result;
};

/**
 * Classis sleep function using async-await
 * @param {Number} s is the number of milliseconds to sleep
 */
export const sleep = async s => new Promise(r => setTimeout(() => { r(); }, s));

/**
 * Checks if the given param exists in the given object
 * @param {object} obj is the object to check if the given param exists in
 * @param {string} param is the param to check if it exists in the given obj
 * @returns {Boolean}
 */
// eslint-disable-next-line max-len
export const itemExists = (obj, param) => Object.prototype.hasOwnProperty.call(
  obj, param,
);
