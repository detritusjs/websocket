import { EventSpewer } from 'detritus-utils';

import { InflateError } from '../errors';

export const DependencyTypes = Object.freeze({
  PAKO: 'pako',
  ZLIB: 'zlib',
});

const ErrorCodes = Object.freeze({
  ERR_ZLIB_BINDING_CLOSED: 'ERR_ZLIB_BINDING_CLOSED',
});

export const Inflate = {
  flushCode: 0,
  module: null as any,
  type: DependencyTypes.ZLIB,
};

for (let type of [DependencyTypes.ZLIB, DependencyTypes.PAKO]) {
  try {
    Inflate.flushCode = Inflate.module!.constants.Z_SYNC_FLUSH;
    Inflate.module = require(type);
    Inflate.type = type;
    break;
  } catch(e) {}
}

export class ZlibDecompressor extends EventSpewer {
  dataChunks: Array<Buffer> = [];
  chunks: Array<Buffer> = [];
  chunkSize: number;
  closed: boolean = false;
  flushing: boolean = false;
  inflate: any = null;
  suffix: Buffer;

  constructor(
    suffix: Buffer,
    chunkSize: number = 64 * 1024,
  ) {
    super();

    if (!Inflate.module) {
      throw new Error(`Missing zlib dependency, pick one: ${JSON.stringify(Object.values(DependencyTypes))}`)
    }

    this.chunkSize = chunkSize;
    this.suffix = suffix;
    this.initialize();
  }

  close(): void {
    this.closed = true;
    this.chunks.length = 0;
    this.dataChunks.length = 0;
    this.flushing = false;

    if (this.inflate) {
      switch (Inflate.type) {
        case DependencyTypes.ZLIB: {
          this.inflate.close();
          this.inflate.removeAllListeners();
        }; break;
      }
    }
    this.inflate = null;
  }

  feed(chunk: Buffer): void {
    if (!this.closed && this.inflate) {
      this.chunks.push(chunk);
      this.write();
    }
  }

  initialize(): void {
    this.close();
    switch (Inflate.type) {
      case DependencyTypes.PAKO: {
        this.inflate = new Inflate.module!.Inflate({
          chunkSize: this.chunkSize,
        });
      }; break;
      case DependencyTypes.ZLIB: {
        this.inflate = Inflate.module!.createInflate({
          chunkSize: this.chunkSize,
          flush: Inflate.flushCode,
        });
        this.inflate.on('data', this.onData.bind(this));
        this.inflate.on('error', this.onError.bind(this));
      }; break;
      default: {
        throw new Error(`Unable to use any ${JSON.stringify(Object.values(DependencyTypes))}`);
      };
    }
    this.closed = false;
  }

  reset(): void {
    this.initialize();
  }

  write(): void {
    if (
      (this.closed) ||
      (!this.inflate) ||
      (!this.chunks.length) ||
      (this.flushing)
    ) {
      return;
    }

    const chunk = <Buffer> this.chunks.shift();
    const isEnd = (
      (this.suffix.length <= chunk.length) &&
      (chunk.slice(-this.suffix.length).equals(this.suffix))
    );

    switch (Inflate.type) {
      case DependencyTypes.PAKO: {
        this.inflate.push(chunk, isEnd && Inflate.flushCode);
        if (isEnd) {
          if (this.inflate.err) {
            const error = new InflateError(this.inflate.msg, this.inflate.err);
            this.onError(error);
          } else {
            this.onData(this.inflate.result);
          }
        }
      }; break;
      case DependencyTypes.ZLIB: {
        this.inflate.write(chunk);
        if (isEnd) {
          this.flushing = true;
          this.inflate.flush(Inflate.flushCode, this.onFlush.bind(this));
          return;
        }
      }; break;
    }
    this.write();
  }

  onData(data: any): void {
    switch (Inflate.type) {
      case DependencyTypes.PAKO: {
        this.emit('data', Buffer.from(data));
      }; break
      case DependencyTypes.ZLIB: {
        this.dataChunks.push(<Buffer> data);
      }; break;
    }
  }

  onError(error: any): void {
    if (error.code === ErrorCodes.ERR_ZLIB_BINDING_CLOSED) {
      // zlib was flushing when we called .close on it
      return;
    }
    this.emit('error', error);
  }

  onFlush(error: any): void {
    if (error) {
      return;
    }
    if (this.dataChunks.length) {
      const chunk = (this.dataChunks.length === 1) ? this.dataChunks.shift() : Buffer.concat(this.dataChunks);
      this.dataChunks.length = 0;
      this.emit('data', chunk);
    }
    this.flushing = false;
    this.write();
  }

  on(event: string | symbol, listener: (...args: any[]) => void): this;
  on(event: 'data', listener: (data: Buffer) => any): this;
  on(event: 'error', listener: (error: Error) => any): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this {
    super.on(event, listener);
    return this;
  }
}
