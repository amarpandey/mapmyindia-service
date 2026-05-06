'use strict';
const mongoose = require('mongoose');

async function connectMongo() {
  const uri = process.env.MONGO_URI;
  if (!uri) throw new Error('MONGO_URI is not set in environment.');

  // Do NOT default the dbName — if the URI already contains a database name
  // (e.g. /di-stage in the Atlas connection string) Mongoose must use that.
  // Only override when MONGO_DB_NAME is explicitly set.
  const base = {
    // Keep TCP connection alive so cloud firewalls don't silently drop it
    socketTimeoutMS:   120_000,
    serverSelectionTimeoutMS: 30_000,
  };
  const opts = process.env.MONGO_DB_NAME
    ? { ...base, dbName: process.env.MONGO_DB_NAME }
    : base;

  await mongoose.connect(uri, opts);

  mongoose.connection.on('disconnected', () =>
    console.warn('[mongodb] Disconnected — Mongoose will auto-reconnect.')
  );
  mongoose.connection.on('reconnected', () =>
    console.log('[mongodb] Reconnected.')
  );

  console.log('✓ MongoDB connected');
}

module.exports = { connectMongo };
