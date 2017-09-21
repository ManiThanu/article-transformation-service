import moment from 'moment';

/**
 * Universal Helpers
 */

/**
 * Converts Timestamp To Persistable UTC Date
 * @param {string|object|date} datetime
 * @returns {Date}
 */
export function toUTC(datetime) {
  // Note: moment will use the current time for undefined values
  if (!datetime) {
    return undefined;
  }
  return moment.utc(datetime).toDate();
}

export default {
  toUTC,
};
