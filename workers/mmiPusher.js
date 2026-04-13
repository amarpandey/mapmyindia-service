'use strict';
/**
 * MapMyIndia (Mappls) IoT Push worker.
 *
 * Watches the `ais140locations` MongoDB collection via a Change Stream and
 * forwards every valid GPS position to the Mappls GPS Push API in real time.
 *
 *   POST https://intouch.mapmyindia.com/iot/api/events/pushData/
 *
 * Protocol reference: protocols/GPS_API_Documentation.docx § 3
 *
 * Behaviour:
 *   • Only packets with a valid GPS fix (gpsValid=true, lat/lng present) are pushed.
 *   • Packets without coordinates (HBT, LGN with no fix, etc.) are silently skipped.
 *   • On HTTP 401 the token is invalidated and the push is retried once.
 *   • On HTTP 403 the IMEI is added to a transient skip-set (cleared on restart).
 *   • On HTTP 429 a warning is logged; the record is not retried.
 *   • Change-stream errors trigger an automatic 5-second reconnect loop.
 *
 * Requires: MongoDB replica-set mode (or Atlas) for change streams.
 */

const axios                         = require('axios');
const mongoose                      = require('mongoose');
const { getToken, invalidateToken } = require('./mmiAuth');

const PUSH_URL = 'https://intouch.mapmyindia.com/iot/api/events/pushData/';

// IMEIs that returned 403 — skip for the process lifetime (not registered on Mappls)
const _skipImeis = new Set();

let _stream = null;

// ── Payload mapper ────────────────────────────────────────────────────────────

/**
 * Maps an ais140locations document to the Mappls IoT JSON body.
 * Returns a single-element array as required by the API.
 *
 * API fields:
 *   timestamp         — Unix epoch seconds of the GPS fix
 *   insertTime        — Unix epoch seconds of when we are sending it
 *   longitude / latitude — WGS-84 decimal degrees
 *   heading           — degrees clockwise from north (0–359)
 *   speed             — km/h
 *   ignition          — 1 = ON, 0 = OFF
 *   gpsOdometer       — cumulative km (optional)
 *   numberOfSatellites — (optional)
 */
function buildPayload(doc) {
  const tsMs = doc.timestamp instanceof Date
    ? doc.timestamp.getTime()
    : new Date(doc.timestamp).getTime();

  const record = {
    timestamp:  Math.floor(tsMs / 1_000),
    insertTime: Math.floor(Date.now() / 1_000),
    longitude:  doc.longitude,
    latitude:   doc.latitude,
    heading:    doc.heading   != null ? Math.round(doc.heading)  : 0,
    speed:      doc.speed     != null ? Math.round(doc.speed)    : 0,
    ignition:   doc.ignition  != null ? (doc.ignition ? 1 : 0)  : 0,
  };

  if (doc.odometer   != null) record.gpsOdometer        = doc.odometer;
  if (doc.satellites != null) record.numberOfSatellites = doc.satellites;

  return [record];
}

// ── HTTP push (with one retry on 401) ─────────────────────────────────────────

async function pushToMMI(imei, payload) {
  for (let attempt = 1; attempt <= 2; attempt++) {
    const token = await getToken();

    let res;
    try {
      res = await axios.post(PUSH_URL, payload, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'trackingCode':  imei,
          'Content-Type':  'application/json',
          'Cookie':        'HttpOnly',
        },
        timeout:        10_000,
        validateStatus: () => true,
      });
    } catch (err) {
      console.error(`[mmi-pusher] IMEI=${imei} network error: ${err.message}`);
      return;
    }

    if (res.status >= 200 && res.status < 300) {
      console.log(`[mmi-pusher] ✓ IMEI=${imei} → HTTP ${res.status}`);
      return;
    }

    if (res.status === 401 && attempt === 1) {
      console.warn(`[mmi-pusher] IMEI=${imei} → 401 stale token — refreshing and retrying…`);
      invalidateToken();
      continue;
    }

    if (res.status === 403) {
      _skipImeis.add(imei);
      console.warn(
        `[mmi-pusher] IMEI=${imei} → 403 Forbidden. ` +
        `IMEI may not be registered on Mappls — skipping for this session.`
      );
      return;
    }

    if (res.status === 429) {
      console.warn(`[mmi-pusher] IMEI=${imei} → 429 Too Many Requests — reduce push rate.`);
      return;
    }

    console.warn(
      `[mmi-pusher] IMEI=${imei} → HTTP ${res.status}: ${JSON.stringify(res.data)}`
    );
    return;
  }
}

// ── Change stream ─────────────────────────────────────────────────────────────

function open() {
  const coll = mongoose.connection.collection('ais140locations');

  _stream = coll.watch(
    [{ $match: { operationType: 'insert' } }],
    { fullDocument: 'default' }
  );

  _stream.on('change', async (event) => {
    const doc  = event.fullDocument;
    if (!doc) return;

    const imei = doc.imei;
    if (!imei) return;

    if (_skipImeis.has(imei)) return;

    // Skip packets with no valid GPS fix
    if (!doc.gpsValid || doc.latitude == null || doc.longitude == null) {
      console.log(
        `[mmi-pusher] IMEI=${imei} packetType=${doc.packetType || '?'} — no GPS fix, skipped.`
      );
      return;
    }

    const payload = buildPayload(doc);

    pushToMMI(imei, payload).catch(err =>
      console.error(`[mmi-pusher] Unexpected error for IMEI=${imei}:`, err.message)
    );
  });

  _stream.on('error', (err) => {
    console.error('[mmi-pusher] Change stream error:', err.message);
    _stream = null;
    setTimeout(open, 5_000);
  });

  _stream.on('close', () => {
    console.warn('[mmi-pusher] Stream closed — reconnecting in 5 s…');
    _stream = null;
    setTimeout(open, 5_000);
  });

  console.log('[mmi-pusher] Watching ais140locations for GPS inserts…');
}

// ── Public API ────────────────────────────────────────────────────────────────

async function start() {
  if (mongoose.connection.readyState !== 1) {
    mongoose.connection.once('connected', open);
  } else {
    open();
  }
}

async function stop() {
  if (_stream) {
    await _stream.close().catch(() => {});
    _stream = null;
  }
}

module.exports = { start, stop };
