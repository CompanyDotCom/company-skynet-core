const { deepParseJson } = require('deep-parse-json');

/**
 * Convert the given SNS type attributes to simple JSON key-value pair of
 * attributes
 * @param {Object} attribs 
 * @returns {[{String: *}]}
 */
// eslint-disable-next-line arrow-body-style
const unmarshallMsgAttribs = attribs => {
  return Object.keys(attribs)
    .reduce((res, key) => {
      const {
        Type: type,
        Value: value,
      } = attribs[key];

      if (type !== 'String' && type !== 'Number') {
        return { ...res, [key]: JSON.parse(value) };
      }
      return { ...res, [key]: value };
    }, {});
};

/**
 * Parse the given SQS message that contains a SNS message to its body,
 * attributes and SQS message receipt handle
 * @param {Object} message
 * @returns { msgBody: Object, msgAttribs: Object, rcptHandle: String}
 */
export const parseMsg = message => {
  let msgB = message.Body ? message.Body : message.body;
  let msgAttribs = {};
  try {
    msgB = message.Body
      ? deepParseJson(message.Body) : deepParseJson(message.body);
  } catch (e1) {
    console.log('Error: withSqsConsumer - parseMsg: Did not get a JSON parsable message in body');
    throw e1;
  }
  if (typeof msgB.MessageAttributes !== 'undefined') {
    msgAttribs = unmarshallMsgAttribs(msgB.MessageAttributes);
  }
  return {
    msgBody: msgB.Message,
    msgAttribs,
    rcptHandle: message.ReceiptHandle,
  };
};

/**
 * Send the given message to the given SQS queue
 * @param {String} qUrl is the url of the queue to send the message to
 * @param {String} msg is the message that needs to be sent
 * @returns {*}
 */
export const sendMsg = async (AWS, region, qUrl, msg) => {
  const sqs = new AWS.SQS({ region });
  return sqs.sendMessage({
    QueueUrl: qUrl,
    MessageBody: msg,
  }).promise();
};

/**
 * Gets messages from the given queue
 * @param {Number} msgCountToFetch is the quantity of messages to fetch from the queue. Returned message quantity can be less than this if the messages in the queue are exhausted
 * @param {String} QueueUrl is the url of the queue from which to fetch the messages
 * @returns {[SQSMessage]}
 */
export const getMsgsFromQueue = async (AWS, region, msgCountToFetch, QueueUrl) => {
  const sqs = new AWS.SQS({ region });
  let messages = [];
  const proms = [];
  let msgsToFetch = msgCountToFetch;
  while (msgsToFetch > 0) {
    const msgsToFetchThisIter = msgsToFetch < 10 ? msgsToFetch : 10;
    msgsToFetch -= msgsToFetchThisIter;
    proms.push(sqs.receiveMessage({
      QueueUrl,
      MaxNumberOfMessages: msgsToFetchThisIter,
      VisibilityTimeout: 900,
      WaitTimeSeconds: 10,
    }).promise());
  }
  const resps = await Promise.all(proms);
  resps.forEach(resp => {
    if (typeof resp.Messages !== 'undefined' && resp.Messages.length > 0) {
      messages = [...messages, ...resp.Messages];
    }
  });
  return messages;
};

/**
 * Delete message from the given queue url using the given receipt handle
 * @param {String} QueueUrl is the url of the queue from which to delete the message
 * @param {String} ReceiptHandle is the receipt handle of the message to be deleted
 * @returns {*}
 */
export const deleteMsg = async (AWS, region, QueueUrl, ReceiptHandle) => {
  const sqs = new AWS.SQS({ region });
  sqs.deleteMessage({
    QueueUrl,
    ReceiptHandle,
  }).promise()
};