import uuid from 'uuid/v4';
import { isArray } from 'util';

import { neverThrowError, itemExists } from './util';
import { fetchRecordsByQuery, batchPutIntoDynamoDb } from './dynamo';
import es from './eventStream';
import {
  deleteMsg as deleteMsgFromQueue,
  sendMsg as sendSqsMsg,
} from './queue';

/**
 * This function processes the message that is sent to it by invoking the message handler function that is passed to it as a param
 * @param {object} AWS is the AWS sdk instance that needs to be passed from the handler
 * @param {string} region is the region of AWS that this service is running in
 * @param {string} service is the name of the service
 * @param { string } account is AWS the account number
 * @param { msgBody: object, msgAttribs: object, rcptHandle: string } is the message object containing properties msgBody (Body of the SNS message), msgAttribs (Message attributes of the SNS message) and rcptHandle (Message receipt handle for the message from SQS)
 * @param {Function} msgHandler is the handler function that will process the message
 * @returns {Boolean}
 * @throws {Error}
 */
export const processMessage = async (AWS, region, service, account,
  { msgBody, msgAttribs, rcptHandle }, msgHandler) => {
  // Use the neverThrowError function from utils to process the message making
  // sure that no error is thrown back

  const dbConfig = await fetchRecordsByQuery(
    AWS,
    {
      TableName: 'vendorConfig',
      ExpressionAttributeNames: { '#pk': 'service' },
      KeyConditionExpression: '#pk = :serv',
      ExpressionAttributeValues: {
        ':serv': { S: `${service}` },
      },
    },
    );
    
  // CR: Mickey: we should think about whether it's preferable to use graphQl here
  //    to pluck just vendor data from the account record.

  // CR: Mickey: this code would be more readable if instead of repeated itemExists calls
  //    we had a helper (like lodash/get) that protected against errors when trying to find
  //    a nested property.  so this could be if(_get(msgBody, 'context.user.accountId')) { ... }
  //    as a simple condition

  // CR: Mickey: is there a case where context.user.accountId is not passed and we
  //    want the request to continue as if it was?  what about context.user.userId?
  //    are there any values on context that we should throw an error without?
  // eslint-disable-next-line no-undef-init
  let serviceUserData = undefined;
  if (itemExists(msgBody, 'context')
      && itemExists(msgBody.context, 'user')
      && itemExists(msgBody.context.user, 'accountId')) {
    const accData = await fetchRecordsByQuery(
      AWS,
      {
        TableName: 'Account',
        ExpressionAttributeNames: { '#pk': 'accountId' },
        KeyConditionExpression: '#pk = :accId',
        ExpressionAttributeValues: {
          ':accId': { S: msgBody.context.user.accountId },
        },
      },
    );
    if (itemExists(accData, 'vendorData')
      && itemExists(accData.vendorData, `${service}`)) {
      serviceUserData = accData.vendorData[`${service}`];
    }
  }

  // CR: Mickey: I would prefer not to arbitrarily abbreviate this variable name
  //    especially when processResponse is later abbreviated differently later in this file.
  //    Wait is this actually `processResult`?  I think that would be the most clear
  //    since the full object is not a response from the handler and also has status details
  //    For files that are /behind the scenes/ I don't feel that the loss of clarity is worth
  //    saving a few keystrokes.
  const procRes = await neverThrowError({
    message: msgBody,
    serviceConfigData: typeof dbConfig !== 'undefined'
      && isArray(dbConfig) && dbConfig.length > 0
      && typeof dbConfig[0].configdata !== 'undefined'
      ? dbConfig[0].configdata : [],
      // CR: Mickey Simpler way to do this access + default: _get(dbConfig, [0, 'configData], [])
      //     Why does this default to [] and not {}.  from current vendorConfig table looks like this is generally Obj
    serviceUserData,
    attributes: msgAttribs,
  }, msgHandler);
  console.log(`processMessage: INFO: Result from worker is ${JSON.stringify(procRes, null, 4)}`);

  // CR: Mickey: is the assumption below that the serviceUserData + account found in the previous
  //    block (ln43-62) may now be out of date?  
  //    I would also lean towards making this a named helper function to make it more
  //    clear what its purpose is like "saveServiceDataToAccount"
  // CR: Mickey: noticed when looking at skynet-generator that this access isn't right
  //     should be: procRes.processResp.serviceUserData to access serviceUserData key returned from handler
  if (itemExists(procRes, 'serviceUserData')) {
    if (typeof procRes.serviceUserData !== 'object') {
      throw new Error('Service specific user data should be an object');
    }
    // CR: Mickey: if we're going to stick with direct DB access, ths exact 15 lines also
    //    exists 40 lines above.  Time for a "getAccount" helper?
    if (itemExists(msgBody, 'context')
      && itemExists(msgBody.context, 'user')
      && itemExists(msgBody.context.user, 'accountId')) {
      const currAccData = await fetchRecordsByQuery(
        AWS,
        {
          TableName: 'Account',
          ExpressionAttributeNames: { '#pk': 'accountId' },
          KeyConditionExpression: '#pk = :accId',
          ExpressionAttributeValues: {
            ':accId': { S: msgBody.context.user.accountId },
          },
        },
      );
      // CR: Mickey: these 10 lines can be replaced with
      // _set(
      //    currAccData,
      //    ['vendorData', service],
      //    { ..._get(currAccData, ['vendorData', service], {}), ...procRes.serviceUserData }
      // )
      if (!itemExists(currAccData, 'vendorData')) {
        currAccData.vendorData = {};
      }
      if (!itemExists(currAccData.vendorData, `${service}`)) {
        currAccData.vendorData[`${service}`] = {};
      }
      currAccData.vendorData[`${service}`] = {
        ...currAccData.vendorData[`${service}`],
        ...procRes.serviceUserData,
      };
      // CR: Mickey: why use batchPut here knowing it will only ever be one value instead of making a single put helper?
      await batchPutIntoDynamoDb(AWS, [currAccData], 'Account');
    }
  }

  // Publish the response SNS event
  await es.publish(
    AWS,
    `arn:aws:sns:${region}:${account}:event-bus`,
    {
      ...msgBody,
      payload: {
        ...msgBody.payload,
        ...procRes.processResp, // CR: Mickey: as noted else where, would love for this to be processResult.handlerResponse
        // CR: Mickey: Looking at skynet-generator I think this should be `...procRes.processResp.res`
      },
    },
    {
      ...msgAttribs,
      status: procRes.status, // CR: Mickey: [disregard: I see this is addressed in `neverThrowError`]
      //    should there be a default value here or an error thrown? 
      //    if procRes.status is undefined.  We probably don't want to publish SNSs without a status
      eventId: uuid(),
      emitter: service,
    },
  );

  // Delete the message from the queue using the rcptHandle, if available.
  // CR: Mickey: What is the case where rcptHandle is not available?  When does that happen
  //    and what happens if the message is not deleted from the queue?
  if (typeof rcptHandle !== 'undefined') {
    await deleteMsgFromQueue(
      AWS,
      region,
      `https://sqs.${region}.amazonaws.com/${account}/${service}-bulktq`,
      rcptHandle,
    );
  }

  // If the status of the process response is 'fail', send the message to Lambda
  // DLQ
  if (procRes.status === 'fail') {
    sendSqsMsg(
      AWS,
      region,
      `https://sqs.${region}.amazonaws.com/${account}/${service}-ldlq`,
      JSON.stringify({
        failedIn: service,
        body: {
          Message: msgBody,
          MessageAttributes: msgAttribs,
          Error: procRes.error,
        },
      }),
    );
  }
  return true;
};
