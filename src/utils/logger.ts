export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, error?: Error, meta?: Record<string, unknown>): void;
}

export class DefaultLogger implements Logger {
  private readonly prefix: string;

  constructor(prefix = '[Notifier]') {
    this.prefix = prefix;
  }

  private format(level: LogLevel, message: string, meta?: Record<string, unknown>): string {
    const timestamp = new Date().toISOString();
    const metaStr = meta && Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
    return `${timestamp} ${this.prefix} [${level.toUpperCase()}]: ${message}${metaStr}`;
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    if (process.env.DEBUG === 'true' || process.env.NODE_ENV === 'development') {
      console.debug(this.format('debug', message, meta));
    }
  }

  info(message: string, meta?: Record<string, unknown>): void {
    console.info(this.format('info', message, meta));
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    console.warn(this.format('warn', message, meta));
  }

  error(message: string, error?: Error, meta?: Record<string, unknown>): void {
    const errorDetails = error ? { name: error.name, message: error.message, stack: error.stack } : undefined;
    console.error(this.format('error', message, { ...meta, error: errorDetails }));
  }
}
