'use strict';
/**
 * MapMyIndia (Mappls) OAuth token manager.
 *
 * Implements the OAuth 2.0 Client Credentials flow described in
 * protocols/GPS_API_Documentation.docx § 2.
 *
 * Environment variables required:
 *   MMI_CLIENT_ID      — your Mappls client_id
 *   MMI_CLIENT_SECRET  — your Mappls client_secret
 *
 * Usage:
 *   const { getToken, invalidateToken } = require('./mmiAuth');
 *   const token = await getToken();   // → Bearer token string
 *   invalidateToken();                // force refresh on next call
 */

const axios = require('axios');

// No trailing slash — matches the working cURL sample in the API doc.
// The server at /token/ issues a redirect which axios follows as a GET,
// dropping all query-string credentials and producing a 401.
const TOKEN_URL = 'https://outpost.mappls.com/api/security/oauth/token';

// ── In-process token cache ────────────────────────────────────────────────────
let _token     = null;  // cached Bearer token string
let _expiresAt = 0;     // absolute ms timestamp when the token expires
let _inflight  = null;  // pending fetch Promise (prevents stampede on concurrent calls)

const REFRESH_BUFFER_MS = 60_000; // refresh 60 s before actual expiry

// ── Internal ──────────────────────────────────────────────────────────────────

async function _fetchToken() {
  const clientId     = process.env.MMI_CLIENT_ID;
  const clientSecret = process.env.MMI_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    throw new Error(
      'MMI_CLIENT_ID and MMI_CLIENT_SECRET must be set in .env to push data to MapMyIndia.'
    );
  }

  const url =
    `${TOKEN_URL}?grant_type=client_credentials` +
    `&client_id=${encodeURIComponent(clientId)}` +
    `&client_secret=${encodeURIComponent(clientSecret)}`;

  const res = await axios.post(url, null, {
    headers:        { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout:        10_000,
    validateStatus: () => true,
  });

  if (res.status !== 200) {
    throw new Error(`MMI auth failed HTTP ${res.status}: ${JSON.stringify(res.data)}`);
  }

  const { access_token, expires_in } = res.data;

  if (!access_token) {
    throw new Error(`MMI auth response missing access_token: ${JSON.stringify(res.data)}`);
  }

  _token     = access_token;
  _expiresAt = Date.now() + (expires_in || 3600) * 1_000;

  console.log(
    `[mmi-auth] Token obtained — expires in ${expires_in}s (~${Math.round(expires_in / 60)} min)`
  );

  return _token;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Returns a valid Bearer token, fetching or refreshing as needed.
 * Multiple concurrent callers share the same in-flight request.
 *
 * @returns {Promise<string>}
 */
async function getToken() {
  if (_token && Date.now() < _expiresAt - REFRESH_BUFFER_MS) {
    return _token;
  }

  if (_inflight) return _inflight;

  _inflight = _fetchToken().finally(() => { _inflight = null; });
  return _inflight;
}

/**
 * Clears the cached token so the next getToken() call fetches a fresh one.
 * Call this when a downstream API request returns HTTP 401.
 */
function invalidateToken() {
  _token     = null;
  _expiresAt = 0;
  console.log('[mmi-auth] Token invalidated — will fetch fresh on next call.');
}

module.exports = { getToken, invalidateToken };
