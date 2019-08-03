import * as os from 'os';
import { URL } from 'url';

import { BaseSocket } from './basesocket';
import { Bucket } from './bucket';
import { Decompressor } from './decompressor';
import { DroppedPacketError } from './errors';
import EventEmitter from './eventemitter';
import { Socket as MediaSocket } from './media';

import {
  ApiVersions,
  CompressTypes,
  DEFAULT_SHARD_COUNT,
  EncodingTypes,
  GatewayDispatchEvents,
  GatewayOpCodes,
  GatewayPresenceStatuses,
  Package,
  SocketCloseCodes,
  ZLIB_SUFFIX,
} from './constants';


const Dependencies: {
  Erlpack: any,
} = {
  Erlpack: null,
};

try {
  Dependencies.Erlpack = require('erlpack');
} catch(e) {}

const IdentifyProperties = Object.freeze({
  '$os': `${os.type()} ${os.release()}; ${os.arch()}`,
  '$browser': process.version.replace(/^v/, (process.release.name || 'node') + '/'),
  '$device': `Detritus v${Package.VERSION}`,
});

const DEFAULT_VOICE_TIMEOUT = 30000;

const defaultOptions = Object.freeze({
  autoReconnect: true,
  compress: CompressTypes.ZLIB,
  encoding: (Dependencies.Erlpack) ? EncodingTypes.ETF : EncodingTypes.JSON,
  guildSubscriptions: true,
  largeThreshold: 250,
  presence: null,
  reconnectDelay: 5000,
  reconnectMax: 5,
  shardCount: 1,
  shardId: 0
});

const defaultPresence = Object.freeze({
  afk: false,
  since: null,
  status: GatewayPresenceStatuses.ONLINE,
});

export class Socket extends EventEmitter {
  _heartbeat: {
    ack: boolean,
    lastAck: null | number,
    lastSent: null | number,
    interval: null | ReturnType<typeof setInterval>,
    intervalTime: null | number,
  } = {
    ack: false,
    lastAck: null,
    lastSent: null,
    interval: null,
    intervalTime: null,
  };
  autoReconnect: boolean;
  bucket: Bucket;
  compress: string;
  disabledEvents: Array<string>;
  discordTrace: Array<any> = [];
  decompressor: Decompressor | null;
  encoding: string;
  guildSubscriptions: boolean;
  killed: boolean = false;
  largeThreshold: number;
  mediaGateways = new Map<string, MediaSocket>();
  presence: PresenceOptions;
  reconnectDelay: number;
  reconnectMax: number;
  reconnects: number = 0;
  resuming: boolean = false;
  sequence: number = 0;
  sessionId: null | string = null;
  shardCount: number;
  shardId: number;
  socket: BaseSocket | null = null;
  token: string;
  url: URL | null = null;
  userId: null | string = null;

  constructor(
    token: string,
    options: SocketOptions = {},
  ) {
    super();

    options = Object.assign({
      disabledEvents: [],
    }, defaultOptions, options);
    if (typeof(options.compress) === 'boolean') {
      if (options.compress) {
        options.compress = CompressTypes.ZLIB;
      } else {
        options.compress = CompressTypes.NONE;
      }
    }
  
    this.autoReconnect = !!options.autoReconnect;
    this.compress = (<string> options.compress).toLowerCase();
    this.encoding = (<string> options.encoding).toLowerCase();
    this.disabledEvents = <Array<string>> options.disabledEvents;
    this.guildSubscriptions = !!options.guildSubscriptions;
    this.largeThreshold = <number> options.largeThreshold;
    this.presence = Object.assign({
      activities: [],
    }, defaultPresence, options.presence);
    this.reconnectDelay = <number> options.reconnectDelay;
    this.reconnectMax = <number> options.reconnectMax;
    this.shardCount = <number> options.shardCount;
    this.shardId = <number> options.shardId;
    this.token = token;

    Object.defineProperty(this, 'token', {enumerable: false, writable: false});

    if (
      (this.compress !== CompressTypes.NONE) &&
      (this.compress !== CompressTypes.ZLIB)
    ) {
      throw new Error(`Compress type must be of: ${CompressTypes.NONE} or ${CompressTypes.ZLIB}`);
    }

    if (this.shardCount <= this.shardId) {
      throw new Error('Shard count cannot be less than or equal to the Shard Id!');
    }

    if (!Object.values(EncodingTypes).includes(this.encoding)) {
      throw new Error(`Invalid Encoding Type, valid: ${JSON.stringify(Object.values(EncodingTypes))}`);
    }

    if (this.encoding === EncodingTypes.ETF && !Dependencies.Erlpack) {
      throw new Error('Install `Erlpack` to use ETF encoding.');
    }

    this.bucket = new Bucket(120, 60 * 1000);

    this.decompressor = null;
    if (this.compress === CompressTypes.ZLIB) {
      this.decompressor = new Decompressor(Buffer.from(ZLIB_SUFFIX));
      this.decompressor.on('data', (data: any) => {
        this.handle(data, true);
      }).on('error', (error: any) => {
        this.disconnect(SocketCloseCodes.INTERNAL_RETRY, 'Invalid data received, reconnecting.');
        this.emit('warn', error);
      });
    }

    Object.defineProperties(this, {
      _heartbeat: {enumerable: false, writable: false},
      killed: {configurable: true},
    });
  }

  get closed(): boolean {
    return !!this.socket && this.socket.closed;
  }

  get closing(): boolean {
    return !!this.socket && this.socket.closing;
  }

  get connected(): boolean {
    return !!this.socket && this.socket.connected;
  }

  get connecting(): boolean {
    return !!this.socket && this.socket.connecting;
  }

  get initializing(): boolean {
    return !this.socket;
  }

  makePresence(
    options?: PresenceOptions,
  ): PresenceData {
    options = Object.assign({
      activities: [],
    }, defaultPresence, this.presence, options);

    const data: PresenceData = {
      activities: options.activities,
      afk: options.afk,
      since: options.since,
      status: options.status,
    };
    if (options.activity || options.game) {
      if (!Array.isArray(data.activities)) {
        data.activities = [];
      }
      if (options.activity) {
        data.activities.unshift(options.activity);
      }
      if (options.game) {
        data.activities.unshift(options.game);
      }
    }
    if (data.activities) {
      data.activities = data.activities.map((activity) => {
        const data: any = {
          application_id: activity.applicationId,
          assets: activity.assets,
          created_at: activity.createdAt,
          details: activity.details,
          flags: activity.flags,
          metadata: activity.metadata,
          name: activity.name,
          party: activity.party,
          secrets: activity.secrets,
          session_id: activity.sessionId,
          state: activity.state,
          sync_id: activity.syncId,
          timestamps: activity.timestamps,
          type: activity.type,
          url: activity.url,
        };
        if (activity.assets) {
          data.assets = {
            large_image: activity.assets.largeImage,
            large_text: activity.assets.largeText,
            small_image: activity.assets.smallImage,
            small_text: activity.assets.smallText,
          };
        }
        if (activity.party) {
          data.party = {
            id: activity.party.id,
            size: activity.party.size,
          };
        }
        if (activity.secrets) {
          data.secrets = {
            id: data.secrets.id,
            size: data.secrets.size,
          };
        }
        if (activity.timestamps) {
          data.timestamps = {
            end: activity.timestamps.end,
            start: activity.timestamps.start,
          };
        }
        return data;
      });
    }
    return data;
  }

  getIdentifyData(): IdentifyData {
    const data: IdentifyData = {
      /* payload compression, rather use transport compression, using the get params overrides this */
      compress: (this.compress === CompressTypes.PAYLOAD),
      guild_subscriptions: this.guildSubscriptions,
      large_threshold: this.largeThreshold,
      properties: IdentifyProperties,
      token: this.token,
    };
    if (DEFAULT_SHARD_COUNT < this.shardCount) {
      data.shard = [this.shardId, this.shardCount];
    }
    if (this.presence) {
      data.presence = this.makePresence();
    }
    return data;
  }

  getResumeData(): ResumeData {
    return {
      seq: this.sequence || null,
      session_id: this.sessionId,
      token: this.token,
    };
  }

  cleanup(
    code?: string | number,
  ): void {
    this.bucket.clear();
    this.bucket.lock();
    if (this.decompressor) {
      this.decompressor.reset();
    }
    if (code === SocketCloseCodes.NORMAL || code === SocketCloseCodes.GOING_AWAY) {
      this.sequence = 0;
      this.sessionId = null;
    }
    if (this._heartbeat.interval !== null) {
      clearInterval(<number> <unknown> this._heartbeat.interval);
      this._heartbeat.interval = null;
    }
    this._heartbeat.ack = false;
		this._heartbeat.lastAck = null;
		this._heartbeat.lastSent = null;
		this._heartbeat.intervalTime = null;
  }

  connect(
    url?: null | string | URL,
  ): void {
    if (this.killed) {return;}
    if (this.connected) {
      this.disconnect();
    }
    if (!url) {
      url = this.url;
    }
    if (!url) {
      throw new Error('Socket requires a url to connect to.');
    }

    this.url = new URL('', <string | URL> url);
    this.url.searchParams.set('encoding', this.encoding);
    this.url.searchParams.set('v', String(ApiVersions.GATEWAY));
    this.url.pathname = this.url.pathname || '/';

    if (this.compress === CompressTypes.ZLIB) {
      this.url.searchParams.set('compress', CompressTypes.ZLIB);
    }

    const ws = this.socket = new BaseSocket(this.url.href);
    this.emit('socket', ws);
    ws.socket.onclose = this.onClose.bind(this);
    ws.socket.onerror = this.onError.bind(this);
    ws.socket.onmessage = this.onMessage.bind(this);
    ws.socket.onopen = this.onOpen.bind(this);
  }

  decode(
    data: any,
    uncompressed: boolean = false,
  ): any {
    try {
      if (data instanceof ArrayBuffer) {
        data = Buffer.from(new Uint8Array(data));
      } else if (Array.isArray(data)) {
        data = Buffer.concat(data);
      }
      if (!uncompressed) {
        if (this.compress === CompressTypes.ZLIB && this.decompressor) {
          this.decompressor.feed(data);
          return null;
        }
      }
      switch (this.encoding) {
        case EncodingTypes.ETF: return Dependencies.Erlpack.unpack(data);
        case EncodingTypes.JSON: return JSON.parse(data);
      }
    } catch(error) {
      this.emit('warn', error);
    }
  }

  disconnect(
    code?: string | number,
    reason?: string,
  ): void {
    this.cleanup(code);
    if (this.socket) {
      this.socket.close(code, reason);
    }
    this.socket = null;
    this.resuming = false;
  }

  handle(
    data: any,
    uncompressed: boolean = false,
  ): void {
    const packet = this.decode(data, uncompressed);
    if (!packet) {return;}
    if (packet.s) {
      const oldSequence = this.sequence;
      const newSequence = packet.s;
      if (oldSequence + 1 < newSequence && !this.resuming) {
        return this.resume();
      }
      this.sequence = newSequence;
    }
    this.emit('packet', packet);

    switch (packet.op) {
      case GatewayOpCodes.HEARTBEAT: {
				this.heartbeat();
			}; break;
			case GatewayOpCodes.HEARTBEAT_ACK: {
				this._heartbeat.ack = true;
				this._heartbeat.lastAck = Date.now();
			}; break;
			case GatewayOpCodes.HELLO: {
				this.setHeartbeat(packet.d);
			}; break;
			case GatewayOpCodes.INVALID_SESSION: {
				setTimeout(() => {
					if (packet.d) {
						this.resume();
					} else {
						this.sequence = 0;
						this.sessionId = null;
						this.identify();
					}
				}, Math.floor(Math.random() * 5 + 1) * 1000);
			}; break;
			case GatewayOpCodes.RECONNECT: {
				this.disconnect(SocketCloseCodes.INTERNAL_RETRY, 'Reconnecting');
				this.connect();
			}; break;
			case GatewayOpCodes.DISPATCH: {
				this.handleDispatch(packet.t, packet.d);
			}; break;
    }
  }

  handleDispatch(
    name: string,
    data: any,
  ): void {
    switch (name) {
      case GatewayDispatchEvents.READY: {
        this.bucket.unlock();
        this.reconnects = 0;
        this.discordTrace = data['_trace'];
        this.sessionId = data['session_id'];
        this.userId = data['user']['id'];
        this.emit('ready');
      }; break;
      case GatewayDispatchEvents.RESUMED: {
        this.reconnects = 0;
        this.resuming = false;
        this.bucket.unlock();
      }; break;
      case GatewayDispatchEvents.GUILD_DELETE: {
        const serverId = <string> data['id'];
        if (this.mediaGateways.has(serverId)) {
          const mGateway = <MediaSocket> this.mediaGateways.get(serverId);
          if (data['unavailable']) {
            mGateway.kill(new Error('The guild this voice was connected to became unavailable'));
          } else {
            mGateway.kill(new Error('Left the guild this voice was connected to'));
          }
        }
      }; break;
      case GatewayDispatchEvents.VOICE_SERVER_UPDATE: {
        const serverId = <string> (data['guild_id'] || data['channel_id']);
        if (this.mediaGateways.has(serverId)) {
          const gateway = <MediaSocket> this.mediaGateways.get(serverId);
          gateway.setEndpoint(data['endpoint']);
          gateway.setToken(data['token']);
        }
      }; break;
      case GatewayDispatchEvents.VOICE_STATE_UPDATE: {
        const userId = <string> data['user_id'];
        if (userId !== this.userId) {
          // not our voice state update
          return;
        }
        const serverId = <string> (data['guild_id'] || data['channel_id']);
        if (this.mediaGateways.has(serverId)) {
          const gateway = <MediaSocket> this.mediaGateways.get(serverId);
          if (!data['channel_id']) {
            gateway.kill();
          } else if (gateway.sessionId !== data['session_id']) {
            gateway.kill(new Error('Connected to this server from a different session'));
          } else {
            gateway.setChannelId(data['channel_id']);
            gateway.resolvePromises();
          }
        }
      }; break;
    }
  }

  kill(): void {
    if (!this.killed) {
      Object.defineProperty(this, 'killed', {value: true});
      this.disconnect(SocketCloseCodes.NORMAL);
      for (let [serverId, socket] of this.mediaGateways) {
        socket.kill();
      }
      this.emit('killed');
    }
  }

  onClose(event: {code: number | string, reason: string}) {
    const code = event.code;
    const reason = event.reason || 'Unknown Reason';
    this.emit('close', {code, reason});
    this.disconnect(code, reason);
    if (this.autoReconnect && !this.killed) {
      if (this.reconnectMax < this.reconnects) {
        this.kill();
      } else {
        setTimeout(() => {
          this.connect();
          this.reconnects++;
        }, this.reconnectDelay);
      }
    }
  }

  onError(event: {error: any} | any) {
    this.emit('warn', event.error);
  }

  onMessage(event: {data: any, type: string}) {
    const { data } = event;
    this.handle(data);
  }

  onOpen() {
    this.emit('open');
    if (this.sessionId) {
      this.resume();
    } else {
      this.identify();
    }
  }

  async ping(timeout?: number): Promise<any> {
    if (!this.connected) {
      throw new Error('Socket is still initializing!');
    }
    return (<BaseSocket> this.socket).ping(timeout);
  }

  send(
    op: number,
    d: any,
    callback?: Function,
    direct: boolean = false,
  ): void {
    const packet = {op, d};

    let data: any;
    try {
      switch (this.encoding) {
        case EncodingTypes.JSON: data = JSON.stringify(packet); break;
        case EncodingTypes.ETF: data = Dependencies.Erlpack.pack(packet); break;
        default: {
          throw new DroppedPacketError(packet, `Invalid encoding: ${this.encoding}`);
        };
      }
    } catch(error) {
      this.emit('warn', error);
    }
    if (data !== undefined) {
      if (direct) {
        if (this.connected) {
          (<BaseSocket> this.socket).send(data, callback);
        } else {
          this.emit('warn', new DroppedPacketError(packet, 'Socket isn\'t connected'));
        }
      } else {
        const throttled = () => {
          if (this.bucket.locked || !this.connected) {
            if (!this.bucket.locked) {
              this.bucket.lock();
            }
            this.bucket.add(throttled, true);
          } else {
            try {
              (<BaseSocket> this.socket).send(data, callback);
            } catch(error) {
              this.emit('warn', error);
            }
          }
        }
        this.bucket.add(throttled);
      }
    }
  }

  heartbeat(fromInterval: boolean = false): void {
    if (fromInterval && !this._heartbeat.ack) {
      this.disconnect(SocketCloseCodes.INTERNAL_RETRY, 'Heartbeat ACK never arrived.');
      this.connect();
    } else {
      const sequence = (this.sequence) ? this.sequence : null;
      this.send(GatewayOpCodes.HEARTBEAT, sequence, () => {
        this._heartbeat.ack = false;
        this._heartbeat.lastSent = Date.now();
      }, true);
    }
  }

  setHeartbeat(data: {
    _trace: any,
    heartbeat_interval: number,
  }): void {
    if (data) {
      this.heartbeat();
      this._heartbeat.ack = true;
      this._heartbeat.lastAck = Date.now();
      this._heartbeat.intervalTime = data['heartbeat_interval'];
      if (this._heartbeat.interval) {
        clearInterval(<number> <unknown> this._heartbeat.interval);
        this._heartbeat.interval = null;
      }
      this._heartbeat.interval = setInterval(this.heartbeat.bind(this, true), data.heartbeat_interval);
      this.discordTrace = data._trace;
    }
  }

  identify(): void {
    const data = this.getIdentifyData();
    this.send(GatewayOpCodes.IDENTIFY, data, undefined, true);
  }

  resume(): void {
    this.resuming = true;
    const data = this.getResumeData();
    this.send(GatewayOpCodes.RESUME, data, undefined, true);
  }

  /* user callable function */

  callConnect(
    channelId: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.CALL_CONNECT, {
      channel_id: channelId,
    }, callback);
  }

  guildStreamCreate(
    guildId: string,
    channelId: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.STREAM_CREATE, {
      channel_id: channelId,
      guild_id: guildId,
      type: 'guild',
    }, callback);
  }

  lobbyConnect(
    lobbyId: string,
    lobbySecret: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.LOBBY_CONNECT, {
      lobby_id: lobbyId,
      lobby_secret: lobbySecret,
    }, callback);
  }

  lobbyDisconnect(
    lobbyId: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.LOBBY_DISCONNECT, {
      lobby_id: lobbyId,
    }, callback);
  }

  lobbyVoiceStatesUpdate(
    voiceStates: Array<{
      lobbyId: string,
      selfDeaf: boolean,
      selfMute: boolean,
    }>,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.LOBBY_VOICE_STATES_UPDATE, voiceStates.map((voiceState) => {
      return {
        lobby_id: voiceState.lobbyId,
        self_deaf: voiceState.selfDeaf,
        self_mute: voiceState.selfMute,
      };
    }), callback);
  }

  requestGuildMembers(
    guildIds: Array<string> | string,
    options: {
      limit: number,
      presences?: boolean,
      query: string,
      userIds?: Array<string>,
    },
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.REQUEST_GUILD_MEMBERS, {
      guild_id: guildIds,
      limit: options.limit,
      presences: options.presences,
      query: options.query,
      user_ids: options.userIds,
    }, callback);
  }

  setPresence(
    options: PresenceOptions,
    callback?: Function,
  ): void {
    const data = this.makePresence(options);
    this.send(GatewayOpCodes.PRESENCE_UPDATE, data, callback);
  }

  streamDelete(
    streamKey: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.STREAM_DELETE, {
      stream_key: streamKey,
    }, callback);
  }

  streamPing(
    streamKey: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.STREAM_PING, {
      stream_key: streamKey,
    }, callback);
  }

  streamSetPaused(
    streamKey: string,
    paused: boolean,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.STREAM_SET_PAUSED, {
      stream_key: streamKey,
      paused,
    }, callback);
  }

  streamWatch(
    streamKey: string,
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.STREAM_WATCH, {
      stream_key: streamKey,
    }, callback);
  }

  updateGuildSubscriptions(
    guildId: string,
    options: {
      activities?: boolean,
      channels?: {[channelId: string]: Array<[number, number]>},
      members?: Array<string>,
      typing?: boolean,
    } = {},
    callback?: Function,
  ) : void {
    this.send(GatewayOpCodes.GUILD_SUBSCRIPTIONS, {
      activities: options.activities,
      channels: options.channels,
      guild_id: guildId,
      members: options.members,
      typing: options.typing,
    }, callback);
  }

  voiceServerPing(callback?: Function) {
    this.send(GatewayOpCodes.VOICE_SERVER_PING, null, callback);
  }

  voiceStateUpdate(
    guildId: null | string = null,
    channelId: null | string = null,
    options: {
      selfDeaf?: boolean,
      selfMute?: boolean,
      selfVideo?: boolean,
    } = {},
    callback?: Function,
  ): void {
    this.send(GatewayOpCodes.VOICE_STATE_UPDATE, {
      channel_id: channelId,
      guild_id: guildId,
      self_deaf: options.selfDeaf,
      self_mute: options.selfMute,
      self_video: options.selfVideo,
    }, callback);
  }

  async voiceConnect(
    guildId?: null | string,
    channelId?: null | string,
    options: {
      forceMode?: string,
      receive?: boolean,
      selfDeaf?: boolean,
      selfMute?: boolean,
      selfVideo?: boolean,
      timeout?: number,
      video?: boolean,
    } = {
      receive: true,
      timeout: DEFAULT_VOICE_TIMEOUT,
    },
  ): Promise<MediaSocket | null> {
    if (!guildId && !channelId) {
      throw new Error('A Guild Id or a Channel Id is required.');
    }

    if (options.timeout === undefined) {
      options.timeout = DEFAULT_VOICE_TIMEOUT;
    }

    if (options.selfVideo && options.video === undefined) {
      options.video = options.selfVideo;
    }

    const serverId = <string> (guildId || channelId);
    let gateway: MediaSocket;
    if (this.mediaGateways.has(serverId)) {
      gateway = <MediaSocket> this.mediaGateways.get(serverId);
      if (!channelId) {
        gateway.kill();
        return null;
      }
      if (channelId === gateway.channelId) {
        return gateway;
      }
    } else {
      if (!channelId) {
        this.voiceStateUpdate(guildId, channelId, options);
        return null;
      }
      gateway = new MediaSocket(this, {
        channelId,
        forceMode: options.forceMode,
        receive: options.receive,
        serverId,
        userId: (<string> this.userId),
        video: options.video,
      });
      this.mediaGateways.set(serverId, gateway);
    }

    let timeout: ReturnType<typeof setTimeout> | null = null;
    if (options.timeout) {
      timeout = setTimeout(() => {
        gateway.kill(new Error(`Voice Gateway took longer than ${options.timeout}ms.`));
        timeout = null;
      }, options.timeout);
    }

    return new Promise((resolve, reject) => {
      gateway.promises.add({resolve, reject});
      this.voiceStateUpdate(guildId, channelId, options);
    }).then(() => {
      if (timeout !== null) {
        clearTimeout(timeout);
      }
      return (channelId) ? gateway : null;
    }).catch((error) => {
      if (timeout !== null) {
        clearTimeout(timeout);
      }
      throw error;
    });
  }
}

export interface SocketOptions {
  autoReconnect?: boolean,
  compress?: boolean | string,
  disabledEvents?: Array<string>,
  encoding?: string,
  guildSubscriptions?: boolean,
  largeThreshold?: number,
  presence?: any,
  reconnectDelay?: number,
  reconnectMax?: number,
  shardCount?: number,
  shardId?: number,
}

interface PresenceActivity {
  applicationId?: string,
  assets?: {
    largeImage?: string,
    largeText?: string,
    smallImage?: string,
    smallText?: string,
  },
  createdAt?: number,
  details?: string,
  flags?: number,
  metadata?: {[key: string]: any},
  name: string,
  party?: {
    id?: string,
    size?: Array<[number, number]>,
  },
  secrets?: {
    join?: string,
    match?: string,
    spectate?: string,
  },
  sessionId?: string,
  state?: string,
  syncId?: string,
  timestamps?: {
    end?: number,
    start?: number,
  },
  type: number,
  url?: string,
}

interface PresenceData {
  activities?: Array<PresenceActivity>,
  afk: boolean,
  game?: PresenceActivity,
  since: number,
  status: string,
}

interface PresenceOptions extends PresenceData {
  activity?: PresenceActivity,
  game?: PresenceActivity,
}

interface IdentifyData {
  compress: boolean,
  guild_subscriptions?: boolean,
  large_threshold: number,
  presence?: PresenceData,
  properties: any,
  shard?: Array<number>,
  token: string,
}

interface ResumeData {
  seq?: null | number,
  session_id: null | string,
  token: string,
}
