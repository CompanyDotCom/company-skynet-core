import { processMessage } from '../library/process';
import { getErrorString } from '../library/util';
import {
  getMsgsFromQueue,
  parseMsg as sqsParser,
} from '../library/queue';
import { getAvaiableCallsThisSec as getAvailableCapacity, incrementUsedCount as incCallCount } from '../library/throttle';

/**
 * This is the handler that is invoked by a cloud watch trigger to process
 * messages that are waiting in the bulk transition queue
 * @param {*} event that invokes the serverless function. In this case, it is a cloud watch trigger
 */
export const handler = async (AWS, { throttleLmts, safeThrottleLimit, reserveCapForDirect, retryCntForCapacity }, region, service, account, event, mHndlr) => {
  try {
    console.log(`bulkTransition: INFO: Scheduled call started. Event is ${event}`);
    
    // Get the available capacity for making calls before going any further
    const availCap = await getAvailableCapacity(
      AWS,
      {
        throttleLmts,
        safeThrottleLimit,
        reserveCapForDirect,
        retryCntForCapacity
      },
      service,
      true);

    // If there is no capacity available, throw back an error and wait for the 
    // function to be re-triggered again by cloud watch
    if (availCap < 1) {
      throw new Error('bulkTransition: ERROR: No capacity to make a call');
    }
    const messagesToProcess = await getMsgsFromQueue(AWS, region, availCap, `https://sqs.${region}.amazonaws.com/${account}/${service}-bulktq`);
    console.log(`bulkTransition: INFO: Processing event ${JSON.stringify(messagesToProcess.length, null, 4)}`);

    if (messagesToProcess.length < 1) {
      return 'bulkTransition: INFO: Processing complete';
    }

    // Increment the 'calls made count' in the database to the number of messages that will be 
    // processed this iteration
    await incCallCount(AWS, service, messagesToProcess.length);

    const proms = [];

    // Push each message call to a promise array
    messagesToProcess.forEach(message => {
      proms.push(processMessage(AWS, region, service, account, sqsParser(message), mHndlr));
    });

    // Await completion of all promises
    await Promise.all(proms);

    return 'bulkTransition: INFO: Processing complete';
  } catch (e) {
    console.log(`bulkTransition: ERROR: ${getErrorString(e)}`);
    throw e;
  }
};
