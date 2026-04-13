'use strict';
require('dotenv').config();

const { connectMongo } = require('./config/mongodb');
const mmiPusher        = require('./workers/mmiPusher');

async function start() {
  // ── Guard: credentials must be present ──────────────────────────────────────
  if (!process.env.MMI_CLIENT_ID || !process.env.MMI_CLIENT_SECRET) {
    console.error(
      '✗ MMI_CLIENT_ID and MMI_CLIENT_SECRET must be set in .env\n' +
      '  Copy .env.example → .env and fill in your Mappls credentials.'
    );
    process.exit(1);
  }

  if (process.env.MMI_ENABLED === 'false') {
    console.warn('⚠ MMI_ENABLED=false — MapMyIndia push is disabled (dry run mode).');
  }

  // ── MongoDB ──────────────────────────────────────────────────────────────────
  await connectMongo();

  // ── MapMyIndia pusher (Change Stream → Mappls IoT API) ──────────────────────
  await mmiPusher.start();

  console.log('\n╔══════════════════════════════════════════════════╗');
  console.log('║  MapMyIndia Service                              ║');
  console.log('║  Watching ais140locations → Mappls IoT API       ║');
  console.log('╚══════════════════════════════════════════════════╝\n');

  // ── Graceful shutdown ────────────────────────────────────────────────────────
  const shutdown = async (signal) => {
    console.log(`\n[${signal}] Shutting down…`);
    await mmiPusher.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

start().catch(err => {
  console.error('Startup failed:', err.message);
  process.exit(1);
});
