const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

export function createLogger(service, level = process.env.LOG_LEVEL || 'info') {
  const min = LEVELS[level] ?? LEVELS.info;

  const write = (lvl, message, meta) => {
    if (LEVELS[lvl] < min) return;
    const line = { time: new Date().toISOString(), level: lvl, service, message };
    if (meta instanceof Error) line.error = meta.stack || meta.message;
    else if (meta && typeof meta === 'object') {
      for (const [key, value] of Object.entries(meta)) if (!(key in line)) line[key] = value;
    }
    (lvl === 'error' || lvl === 'warn' ? process.stderr : process.stdout).write(`${JSON.stringify(line)}\n`);
  };

  return {
    debug: (msg, meta) => write('debug', msg, meta),
    info: (msg, meta) => write('info', msg, meta),
    warn: (msg, meta) => write('warn', msg, meta),
    error: (msg, meta) => write('error', msg, meta),
  };
}
