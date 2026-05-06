'use strict';

const axios                         = require('axios');
const mongoose                      = require('mongoose');
const { getToken, invalidateToken } = require('./mmiAuth');

const PUSH_URL = 'https://intouch.mapmyindia.com/iot/api/events/pushData/';

// IMEIs that returned 403 — skip for the process lifetime (not registered on Mappls)
const _skipImeis = new Set();

let _stream        = null;
let _reconnecting  = false;   // guard against duplicate reconnect timers
let _lastEventAt   = Date.now();
let _activeOps     = 0;       // count of in-flight pushToMMI calls
let _watchdogTimer = null;

const MAX_CONCURRENT_PUSH = 20;  // drop events if more than this many are in-flight
const WATCHDOG_INTERVAL   = 5 * 60 * 1_000;  // check every 5 min
const STALE_STREAM_MS     = 15 * 60 * 1_000; // treat stream as dead after 15 min silence

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
  _activeOps++;
  try {
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
  } finally {
    _activeOps--;
  }
}

// ── Watchdog: detects a silently-dead change stream and forces reconnect ──────

function startWatchdog() {
  if (_watchdogTimer) clearInterval(_watchdogTimer);

  _watchdogTimer = setInterval(() => {
    const idleMs  = Date.now() - _lastEventAt;
    const idleMins = Math.round(idleMs / 60_000);
    console.log(
      `[mmi-pusher] heartbeat — stream=${_stream ? 'open' : 'null'} ` +
      `idle=${idleMins}m in-flight=${_activeOps}`
    );

    if (!_stream && !_reconnecting) {
      console.warn('[mmi-pusher] Watchdog: no stream — forcing reconnect.');
      scheduleReconnect(0);
      return;
    }

    if (_stream && idleMs > STALE_STREAM_MS) {
      console.warn(
        `[mmi-pusher] Watchdog: stream idle for ${idleMins} min — closing and reconnecting.`
      );
      const dying = _stream;
      _stream = null;
      dying.close().catch(() => {});
      scheduleReconnect(0);
    }
  }, WATCHDOG_INTERVAL);
}

function scheduleReconnect(delayMs = 5_000) {
  if (_reconnecting) return;
  _reconnecting = true;
  setTimeout(() => {
    _reconnecting = false;
    open();
  }, delayMs);
}

// ── Change stream ─────────────────────────────────────────────────────────────

function open() {
  if (_stream) return; // already open

  if (mongoose.connection.readyState !== 1) {
    console.warn('[mmi-pusher] MongoDB not ready — waiting for connection before opening stream.');
    mongoose.connection.once('connected', open);
    return;
  }

  const coll = mongoose.connection.collection('ais140locations');

  _stream = coll.watch(
    [{ $match: { operationType: 'insert' } }],
    { fullDocument: 'default' }
  );

  _lastEventAt = Date.now(); // reset idle clock on fresh open

  _stream.on('change', async (event) => {
    _lastEventAt = Date.now();

    const doc  = event.fullDocument;
    if (!doc) return;

    const imei = doc.imei;
    if (!imei) return;

    if (_skipImeis.has(imei)) return;

    // Skip packets with no valid GPS fix
    if (!doc.gpsValid || doc.latitude == null || doc.longitude == null) {
      return;
    }

    // Shed load when too many pushes are already in-flight
    if (_activeOps >= MAX_CONCURRENT_PUSH) {
      console.warn(
        `[mmi-pusher] IMEI=${imei} dropped — ${_activeOps} pushes already in-flight (backpressure).`
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
    scheduleReconnect(5_000);
  });

  _stream.on('close', () => {
    console.warn('[mmi-pusher] Stream closed — reconnecting in 5 s…');
    _stream = null;
    scheduleReconnect(5_000);
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
  startWatchdog();
}

async function stop() {
  if (_watchdogTimer) {
    clearInterval(_watchdogTimer);
    _watchdogTimer = null;
  }
  if (_stream) {
    await _stream.close().catch(() => {});
    _stream = null;
  }
}

module.exports = { start, stop };
