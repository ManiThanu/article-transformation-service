/* eslint-disable arrow-body-style, consistent-return, key-spacing */

import assert       from 'assert';
import messaging    from 'msyn-messaging';
import rqsHelpers   from '../helpers/rqs';

/**
 * Mappings Consumer
 * @desc: Saves Asset Mappings from RQS
 */

const Connection = messaging.connections.Default;
const Receiver   = messaging.receivers.Default;

/**
 * Detects Mapping Fields To Be Updated
 * @param {string} assetType
 * @param {object} asset
 * @returns {object} fields - persistable mapping key/values
 */
function fieldsForAssetMappingUpdate(assetType, asset) {
  switch (assetType) {
    case 'story':
      return {
        revision_id:        asset.revision_id,
        group_updated_at:   asset.group_updated_at,
        story_updated_at:   asset.story_updated_at,
        group_published_at: asset.group_published_at,
        story_published_at: asset.story_published_at,
      };
    case 'image':
      return {
        published_at: asset.published_at,
        url:          asset.url,
      };
    default:
      throw new Error(`Unsupported Asset Type: ${assetType}`);
  }
}

async function consume() {
  try {
    const connection = new Connection();

    // Saves New Mappings From RQS
    const receiver = new Receiver({
      connection,
      headers: { 'activemq.prefetchSize': JSON.parse(process.env.ASSET_MAPPINGS_MAX_FETCH || 100) },
      destination: process.env.ASSET_MAPPINGS_DESTINATION,
      acknowledge: JSON.parse(process.env.ASSET_MAPPINGS_ACK || true),
      processor: async (message) => {
        // Parse
        const { headers, payload: { assetId: assetBBID, queryIds: feedIDs = [] } } = message;
        assert(headers.asset_type && assetBBID, 'RQS payload missing "asset_type" Header or assetID');

        // Detect Asset & Mapping Types
        const assetType   = rqsHelpers.convertAssetType(headers.asset_type);
        const mappingType = `${assetType}Mapping`;

        // Log
        console.log(`Saving ${feedIDs.length} ${mappingType}s for asset ${assetBBID} ...`);

        // Find Asset
        const asset = (await Asset[assetType].findOne({ bb_id: assetBBID })).attributes;

        // Update Mappings
        for (const feedID of feedIDs) {
          try {
            await AssetMapping[mappingType].upsert({
              bb_id: assetBBID,
              feed_id: feedID,
              asset_id: asset.id,
              asset_type_id: asset.asset_type_id,
              asset_format_id: asset.asset_format_id,
            }, fieldsForAssetMappingUpdate(assetType, asset));
          } catch (e) {
            // TODO: Determine if rescuing errors and allowing the rest to process is desired
            console.error(`Error Saving ${mappingType}: { bb_id: ${assetBBID}, feed_id: ${feedID} } :`, e.message);
          }
        }

        // TODO: Send to FTP Push Delivery Service
      },
    });
    await receiver.connect();
    await receiver.receive();
  } catch (e) {
    console.log('Uncaught Error: ', e.message);
  }
}

consume();
