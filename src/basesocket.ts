import { BaseCollection, EventSpewer, Timers } from 'detritus-utils';

import {
  SocketCloseCodes,
  SocketEventsBase,
} from './constants';


export enum DependencyTypes {
  UWS = 'uws',
  WS = 'ws',
}

export const WebsocketDependency: {
  module: any,
  type: null | DependencyTypes,
} = {
  module: null,
  type: null,
};

[
  DependencyTypes.WS,
  DependencyTypes.UWS,
].forEach((dependency) => {
  try {
    WebsocketDependency.module = require(dependency);
    WebsocketDependency.type = dependency;
  } catch(e) {}
});

export class BaseSocket extends EventSpewer {
  readonly pings = new BaseCollection<string, {
    reject: Function,
    resolve: Function,
  }>();

  socket: any;

  constructor(url: string) {
    super();

    if (WebsocketDependency.module === null) {
      throw new Error(`Missing a WebSocket Dependency, pick one: ${JSON.stringify(Object.values(DependencyTypes))}`)
    }

    this.socket = new WebsocketDependency.module(url);
    this.socket.on(SocketEventsBase.CLOSE, this.onClose.bind(this));
    this.socket.on(SocketEventsBase.PONG, this.onPong.bind(this));

    this.socket.on(SocketEventsBase.ERROR, this.emit.bind(this, SocketEventsBase.ERROR));
    this.socket.on(SocketEventsBase.MESSAGE, this.emit.bind(this, SocketEventsBase.MESSAGE));
    this.socket.on(SocketEventsBase.OPEN, this.emit.bind(this, SocketEventsBase.OPEN));
    this.socket.on(SocketEventsBase.PING, this.emit.bind(this, SocketEventsBase.PING));
  }

  get closed(): boolean {
    return this.socket.readyState === this.socket.CLOSED;
  }

  get closing(): boolean {
    return this.socket.readyState === this.socket.CLOSING;
  }

  get connected(): boolean {
    return this.socket.readyState === this.socket.OPEN;
  }

  get connecting(): boolean {
    return this.socket.readyState === this.socket.CONNECTING;
  }

  get using(): DependencyTypes {
    if (!WebsocketDependency.type) {
      throw new Error(`Missing a WebSocket Dependency, pick one: ${JSON.stringify(Object.values(DependencyTypes))}`);
    }
    return WebsocketDependency.type;
  }

  send(data: any, callback?: Function): void {
    if (this.connected) {
      this.socket.send(data, {}, callback);
    }
  }

  close(code: number = SocketCloseCodes.NORMAL, reason: string = ''): void {
    if (this.connected) {
      this.socket.close(code, reason);
    }
  }

  onClose(code: number, message: string): void {
    for (const [nonce, {reject}] of this.pings) {
      reject(new Error('Socket has closed.'));
      this.pings.delete(nonce);
    }
    this.pings.clear();

    this.socket.removeAllListeners();
    this.emit(SocketEventsBase.CLOSE, code, message);

    this.removeAllListeners();
  }

  onPong(data: any): void {
    try {
      const { nonce }: { nonce: string } = JSON.parse(String(data));
      const ping = this.pings.get(nonce);
      if (ping) {
        ping.resolve();
        this.pings.delete(nonce);
      }
    } catch(e) {
      // malformed ping?
    }
    this.emit(SocketEventsBase.PONG, data);
  }

  async ping(
    timeout: number = 1000,
  ): Promise<number> {
    if (!this.connected) {
      throw new Error('Socket isn\'t connected.');
    }
    const nonce = `${Date.now()}.${Math.random().toString(36)}`;
    return new Promise((resolve, reject) => {
      const expire = new Timers.Timeout();
      if (timeout) {
        expire.start(timeout, () => {
          this.pings.delete(nonce);
          reject(new Error(`Pong took longer than ${timeout}ms.`));
        });
      }

      const now = Date.now();
      new Promise((res, rej) => {
        this.pings.set(nonce, {resolve: res, reject: rej});
        this.socket.ping(JSON.stringify({nonce}));
      }).then(() => {
        expire.stop();
        resolve(Math.round(Date.now() - now));
      });
    });
  }

  terminate() {
    return this.socket.terminate();
  }
}
