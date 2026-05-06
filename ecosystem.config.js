module.exports = {
  apps: [
    {
      name:             'mapmyindia-service',
      script:           'index.js',
      // Restart if the process uses more than 300 MB (memory leak safety net)
      max_memory_restart: '300M',
      // Restart the process every 2 hours regardless (matches user requirement)
      cron_restart:     '0 */2 * * *',
      // If it crashes, wait 5 s before restarting
      restart_delay:    5_000,
      // Expose NODE_ENV so .env is still used for secrets
      env: {
        NODE_ENV: 'production',
      },
      // Stream logs to PM2 log files
      out_file:  './logs/out.log',
      error_file: './logs/error.log',
      merge_logs: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
    },
  ],
};
