/**
 * RQS (Reverse Query Service) Helpers
 */

/**
 * Toggles To/From RQS's Asset Type & Asset Service's Asset Type
 * @desc: Currently 'asset_type' header value for messages sent to ASSET_MAPPINGS_DESTINATION
 * @param {string} assetType
 */
function convertAssetType(assetType) {
  switch (assetType) {
    // Asset Service => RQS
    case 'story':
      return 'ARTICLE';
    case 'image':
      return 'PHOTO';
    case 'video':
      return 'VIDEO';
    // RQS => Asset Service
    case 'ARTICLE':
      return 'story';
    case 'PHOTO':
      return 'image';
    case 'VIDEO':
      return 'video';
    default:
      throw new Error(`Unsupported Asset Type: ${assetType}`);
  }
}

/**
 * Detects Asset Type Used By RQS Meta Queue
 * @desc: Currently the suffix appended to 'ASSET_MAPPINGS_DESTINATION'
 * @param {string} assetType
 */
function detectMetaQueueSuffix(assetType) {
  switch (assetType) {
    case 'story':
      return 'articles';
    case 'image':
      return 'photos';
    case 'video':
      return 'videos';
    default:
      throw new Error(`Unsupported Asset Type: ${assetType}`);
  }
}

/**
 * Reformats The Asset Meta Payload To Meet The Needs of RQS
 * @see: The story payload structure should be the same as Content Service
 */
function buildMetaPayload(assetType, payload) {
  switch (assetType) {
    case 'story':
      return {
        value: {
          ...payload,
          revision: payload.id,
          headlines: Object.assign({ story: '' }, payload.headline),
          tags: {
            niCodes: payload.niCodes.ni.map(id => ({ id })),
            tickers: payload.tickers.map(ticker => ({ id: ticker.id })),
          },
        },
      };
    case 'image':
      return {
        value: {
          ...payload,
          revision: payload.id,
        },
      };
    case 'video':
      return { value: payload };
    default:
      throw new Error(`Unsupported Asset Type: ${assetType}`);
  }
}

export default {
  convertAssetType,
  detectMetaQueueSuffix,
  buildMetaPayload,
};
