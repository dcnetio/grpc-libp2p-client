import type { Libp2p } from "libp2p";
import { HTTP2Parser } from "./dc-http2/parser.js";
import { StreamWriter } from "./dc-http2/stream.js";
import { Http2Frame } from "./dc-http2/frame.js";
import { HPACK } from "./dc-http2/hpack.js";
import { createAbortController, createAbortError } from "./abort.js";

import type { Multiaddr } from "@multiformats/multiaddr";

const dialTimeout = 5000; // 5秒
// 仅用于「打开流」本身，不覆盖对端首字节的等待时间。
// 发布环境经过公网 WSS/Relay 建流时，协议协商可能明显慢于本地开发节点。
// 10 秒会把仍在恢复的连接误判为 signal timed out，并让所有并行请求一起重拨。
const STREAM_OPEN_TIMEOUT_MS = 20000;
// 流开出来之后，对端多久没发过任何一帧就判定这条连接已经不通。
// 按 HTTP/2，对端在收到 preface 后应当立即回 SETTINGS，这与应用层算得快慢无关，
// 所以这个值可以放得很宽也不会误判「节点在跑构建、响应慢」。
const PEER_SILENCE_TIMEOUT_MS = 15000;
const DEFAULT_SEND_WINDOW_TIMEOUT = 30000;
const CALL_CLEANUP_TIMEOUT = 5000;
// 关闭流的最长等待。libp2p 的 stream.close() 会等待服务端半关闭确认，
// 网络异常时可能永久挂起——此时底层 muxer 流仍占用 maxOutboundStreams 名额，
// 大量重试累积的半开流最终触发 "too many outbound protocol streams" 重连风暴。
const STREAM_CLOSE_TIMEOUT_MS = 5000;

/**
 * muxer 是否已经死了。
 *
 * libp2p 的 Connection.status 取自 maConn（底层 socket），而 newStream 第一件事是
 * 校验 `muxer.status !== 'open'` 并抛 `The connection muxer is "closing" and not
 * "open"`。yamux 先关、socket 后关的窗口里，这条连接的 status 仍然是 'open'：
 * 它「看着好好的、开流必炸」。控制台里同一个 ConnectionClosedError 刷几十条、
 * 换页面重进才恢复，就是这种僵尸连接被反复取用的结果。
 *
 * 用结构化取值而不是引用 libp2p 类型：本库允许被另一份 @libp2p/interface 安装
 * 消费，拿不到 muxer 字段时按「还活着」处理，不改变旧行为。
 */
function isConnectionMuxerDead(connection: Connection): boolean {
  const status = (connection as unknown as { muxer?: { status?: string } })
    .muxer?.status;
  return status != null && status !== "open";
}

/**
 * 判断池中连接是否还能复用。
 * abort() 会把 status 置为 'aborted' 并写入 timeline.close，但它只转发 maConn 的
 * close 事件、不转发 abort，所以 abort 掉的连接不会触发池的 close 监听，只能在
 * 取用时主动判定。timeline.close 作为兜底，覆盖 status 未及时更新的传输实现。
 */
function isConnectionReusable(connection: Connection): boolean {
  const status = connection.status;
  if (status && status !== "open") {
    return false;
  }
  if (connection.timeline?.close != null) {
    return false;
  }
  if (isConnectionMuxerDead(connection)) {
    return false;
  }
  return true;
}

function getAbortError(signal?: AbortSignal): Error {
  const reason = signal?.reason;
  return reason instanceof Error ? reason : new Error("Operation aborted");
}

/**
 * `newStream` 超时与业务 RPC 响应超时不同：此时连协议协商都没有完成。
 * libp2p 的连接状态可能仍显示为 open，但下一次开流通常还会命中同一条半死连接。
 */
function isStreamOpenTimeout(error: unknown): boolean {
  if (error == null) return false;
  const candidate = error as { name?: unknown; message?: unknown };
  const name = String(candidate.name || "");
  const message = String(candidate.message || error);
  return /signal timed out/i.test(message) && /timeout/i.test(name || message);
}

/**
 * 立刻把一条僵尸连接从 libp2p 的连接表里赶出去。
 *
 * 必须是 abort 而不是 close：close() 是优雅关闭，要等对端确认，muxer 都没了的
 * 连接上它要么以 `AggregateError: All promises were rejected` 失败、要么直接挂住；
 * 只要 maConn.status 还是 'open'，connection-manager 的 findExistingConnection
 * （只看 status，不看 muxer）就会把这条连接继续发给下一次 dial——于是「摘出连接池
 * 再重拨」拿回来的还是它。abort() 同步把 status 置为非 open，dial 才会真的重连。
 */
function discardDeadConnection(connection: Connection, reason: string): void {
  try {
    connection.abort(new Error(reason));
  } catch {
    /* 已经关闭，忽略 */
  }
}

/** 丢弃不可用连接时顺手释放底层资源，避免 muxer 名额泄漏。 */
async function safeCloseConnection(connection: Connection): Promise<void> {
  if (isConnectionMuxerDead(connection)) {
    discardDeadConnection(connection, "unusable pooled connection");
    return;
  }
  try {
    await connection.close();
  } catch {
    discardDeadConnection(connection, "unusable pooled connection");
  }
}

/**
 * 解码 gRPC `grpc-message` trailer。
 * 按 gRPC 协议规范，该字段使用百分号编码（percent-encoding）承载 UTF-8 字节，
 * 非 ASCII 字符（如中文错误信息）会以 %XX 形式出现。直接透传会导致上层拿到
 * 形如 "%E8%AF%B7..." 的乱码，因此在读取时统一解码为可读文本。
 * 解码失败时回退原始字符串，保证健壮性。
 */
function decodeGrpcMessage(message: string): string {
  if (!message || message.indexOf("%") === -1) {
    return message;
  }
  try {
    const isHex = (c: string): boolean =>
      (c >= "0" && c <= "9") ||
      (c >= "a" && c <= "f") ||
      (c >= "A" && c <= "F");
    const bytes: number[] = [];
    for (let i = 0; i < message.length; i++) {
      const ch = message[i];
      // 仅当 % 后紧跟两个合法十六进制字符时才按 percent-encoding 解码，
      // 避免 parseInt 对 "-d" 等非法序列的符号/部分解析导致字节错乱。
      if (
        ch === "%" &&
        i + 2 < message.length &&
        isHex(message[i + 1]) &&
        isHex(message[i + 2])
      ) {
        bytes.push(parseInt(message.substr(i + 1, 2), 16));
        i += 2;
        continue;
      }
      bytes.push(message.charCodeAt(i) & 0xff);
    }
    return new TextDecoder("utf-8").decode(new Uint8Array(bytes));
  } catch {
    return message;
  }
}

type CallMode =
  | "unary"
  | "server-streaming"
  | "client-streaming"
  | "bidirectional";
type TransportProfile = "flow-control" | "compatibility";
// Derive transport types from the injected Libp2p instance. Importing
// Connection/Stream directly from @libp2p/interface makes a linked consumer
// with another compatible package instance fail type-checking because the two
// dependency copies carry different generic/private identities.
type Connection = Awaited<ReturnType<Libp2p["dial"]>>;
type Stream = Awaited<ReturnType<Connection["newStream"]>>;

/**
 * 优雅关闭 libp2p 流，但不允许 close() 在网络异常时无限挂起。
 * close() 会等待服务端半关闭确认；超过 STREAM_CLOSE_TIMEOUT_MS 仍未完成，
 * 就强制 abort()——立即向 muxer 发送 RST 释放该流的名额，防止半开流堆积。
 * 幂等且吞掉所有错误：清理路径不应再抛出。
 */
async function closeStreamWithTimeout(
  stream: Stream,
  timeoutMs = STREAM_CLOSE_TIMEOUT_MS,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  try {
    await Promise.race([
      stream.close(),
      new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          timedOut = true;
          resolve();
        }, timeoutMs);
      }),
    ]);
  } catch {
    // close() 抛错通常意味着流已被 abort，无需再处理。
    return;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
  if (timedOut) {
    try {
      stream.abort(new Error("stream close timeout"));
    } catch {
      /* abort 幂等，忽略 */
    }
  }
}

interface CallOptions {
  batchSize?: number;
  maxBatchWaitMs?: number;
  freshConnection?: boolean;
  transportProfile?: TransportProfile;
}

interface ConnectionState {
  activeStreams: number;
  maxConcurrentStreams: number;
  waiters: Array<{ resolve: () => void; reject: (error: Error) => void }>;
  /** 是否已收到 GOAWAY：此后不再复用该连接开新流，等既有流排空后关闭。 */
  goawayReceived: boolean;
  /**
   * 是否已被判定为不可用（开流失败）：与 GOAWAY 同样的处理——摘出连接池不再复用，
   * 等既有流排空后关闭。status 仍是 'open' 但 multistream-select 不再应答的连接
   * 只能这样识别，isConnectionReusable 看不出来。
   */
  evicted: boolean;
  /** 关闭是否已发起，保证排空关闭只执行一次。 */
  closeIssued: boolean;
  /**
   * 已在申请槽位、但尚未计入 activeStreams 的调用数。
   *
   * notifyStreamSlotAvailable 会先把 waiter 从队列 shift 出来再 resolve，等待者
   * 恢复执行前 waiters 已空且 activeStreams 可能已归零；若此时判定「已排空」就会
   * 关掉马上要被使用的连接。该计数覆盖「拿到槽位到 activeStreams 自增」之间的窗口。
   */
  pendingStreams: number;
}

class StreamManager {
  currentStreamId: number;
  private streamIdLock: Promise<void> = Promise.resolve();

  constructor() {
    this.currentStreamId = 1; // 从 1 开始，以模拟奇数 ID
  }

  async getNextAppLevelStreamId(): Promise<number> {
    // 使用 Promise 链来确保原子性操作
    return new Promise<number>((resolve) => {
      this.streamIdLock = this.streamIdLock.then(() => {
        const id = this.currentStreamId;
        this.currentStreamId += 2; // 确保奇数步进
        resolve(id);
      });
    });
  }
}

/**
 * 连接池、每连接状态、每连接流号分配器都必须是**模块级**的。
 *
 * dcapi 的每个 RPC 方法里都是 `new Libp2pGrpcClient(...)`（全库 70+ 处），所以
 * 这些表一旦挂在实例上，每次调用拿到的都是空表：
 *  - 连接池永不命中，每次调用都重新 dial；而 libp2p 对同一个 peer 的 dial 会复用
 *    同一条 Connection，于是 N 个并发调用**各自认为这条连接上只有自己一条流**，
 *    maxConcurrentStreams 限流从不生效，一路把对端的流上限撑爆（表现为
 *    TooManyOutboundProtocolStreams / 开不出新流 / 一直拉不下来，出去再回来又好了）；
 *  - 摘池、GOAWAY 排空、静默看门狗同理：一个实例标记的驱逐，另一个实例看不见。
 *
 * 连接本身与 protocol / token 无关（两者只在 newStream 与请求头上用），按
 * 「本地节点 + 目标地址」做键即可跨实例共享。
 */
const sharedConnectionStreamManagers: WeakMap<object, StreamManager> =
  new WeakMap();
const sharedConnectionStates: WeakMap<object, ConnectionState> = new WeakMap();
const sharedConnectionPool: Map<
  string,
  { promise: Promise<Connection>; connection?: Connection }
> = new Map();

export class Libp2pGrpcClient {
  node: Libp2p;
  protocol: string;
  peerAddr: Multiaddr;
  token: string;

  constructor(
    node: Libp2p,
    peerAddr: Multiaddr,
    token: string,
    protocol?: string
  ) {
    this.node = node;
    this.peerAddr = peerAddr;
    if (protocol) {
      this.protocol = protocol;
    } else {
      this.protocol = "/dc/thread/0.0.1";
    }
    this.token = token;
  }

  /** 连接池键：同一个本地节点 + 同一个目标地址才算同一条连接 */
  private get poolKey(): string {
    const localId = (this.node as { peerId?: { toString(): string } }).peerId;
    return `${localId ? localId.toString() : "node"}|${this.peerAddr.toString()}`;
  }

  private getStreamManagerFor(connection: object): StreamManager {
    let manager = sharedConnectionStreamManagers.get(connection);
    if (!manager) {
      manager = new StreamManager();
      sharedConnectionStreamManagers.set(connection, manager);
    }
    return manager;
  }

  private getConnectionState(connection: object): ConnectionState {
    let state = sharedConnectionStates.get(connection);
    if (!state) {
      state = {
        activeStreams: 0,
        maxConcurrentStreams: Number.POSITIVE_INFINITY,
        waiters: [],
        goawayReceived: false,
        evicted: false,
        closeIssued: false,
        pendingStreams: 0,
      };
      sharedConnectionStates.set(connection, state);
    }
    return state;
  }

  /**
   * GOAWAY 后的「排空关闭」。
   *
   * 按 HTTP/2 语义（RFC 9113 §6.8）GOAWAY 表示「不要再开新流」，ID 不大于
   * lastStreamId 的既有流有权继续跑完；errorCode=NO_ERROR 更只是优雅下线通知。
   * 本库中每个 Call 各自开一条 libp2p 流并在其上跑独立的 HTTP/2 会话，因此一条
   * GOAWAY 的作用域仅限它自己那条流——直接 connection.close() 会把复用同一条
   * libp2p 连接的其他并发 Call 一起拆掉（并行生成时表现为另一路流突然中断）。
   *
   * 所以收到 GOAWAY 只做标记（连接已从池中摘除，不会再被复用），由最后一条流
   * 结束时调用本方法真正关闭，既不误伤兄弟流，也不泄漏连接。
   */
  private closeConnectionIfDrained(
    connection: Connection | null,
    state: ConnectionState | null
  ) {
    if (!connection || !state) return;
    if ((!state.goawayReceived && !state.evicted) || state.closeIssued) return;
    // 三者都为空才算真正排空：活跃流、排队等槽位的调用、已拿到槽位但尚未计数的调用。
    if (
      state.activeStreams > 0 ||
      state.waiters.length > 0 ||
      state.pendingStreams > 0
    ) {
      return;
    }
    state.closeIssued = true;
    if (isConnectionMuxerDead(connection)) {
      // muxer 已经没了，close() 只会失败或挂住，而只要 maConn 还是 'open'，
      // libp2p 的 dial 就会继续把这条连接发回来。
      discardDeadConnection(connection, "connection muxer already closed");
      return;
    }
    void connection.close().catch((err) => {
      console.warn("Error closing drained connection after GOAWAY:", err);
      // close() 失败后连接往往仍停在 'open'，findExistingConnection 会一直挑中它。
      // 必须 abort 兜底，否则「关不掉的连接」就是下一轮报错的源头。
      discardDeadConnection(connection, "graceful close failed");
    });
  }

  /**
   * 把连接池里正是这一条的记录摘掉。
   *
   * 只认 connection 相同的条目：条目里没有 connection 时它是一次进行中的重拨，
   * 那多半就是要替换它的新连接，按 key 无脑删会让并发调用各拨各的。
   */
  private dropPooledConnection(connection: Connection) {
    const key = this.poolKey;
    const pooled = sharedConnectionPool.get(key);
    if (pooled && pooled.connection === connection) {
      sharedConnectionPool.delete(key);
    }
  }

  /**
   * 「对端一帧都没发过」看门狗。
   *
   * newStream 成功不等于连接可用：multistream-select 应答了、HTTP/2 会话却一个字节
   * 都不回的连接，status 仍是 'open'，开流也不报错，只会让每一次调用等满自己的超时
   * （上传 60 秒、拉构建产物 60 秒 ×3），用户看到的就是「一直拉不下来」。
   *
   * 判据取「有没有收到任意一帧」而不是「响应有没有回来」：按 HTTP/2，对端在流建立
   * 后立即发 SETTINGS，跟应用层算得多慢无关，所以慢节点不会被误判。
   *
   * 只摘池、不打断本次调用：本次仍按自己的超时走完，避免改变调用语义；真正的收益
   * 在于下一次调用（含上层的重试）会拨一条新连接，而不是继续复用这条死的。
   */
  private armPeerSilenceWatchdog(
    parser: HTTP2Parser,
    connection: Connection | null,
    state: ConnectionState | null,
    tag: string
  ) {
    if (!connection) return;
    const timer = setTimeout(() => {
      this.evictConnection(
        connection,
        state,
        `${tag}: peer sent no frames within ${PEER_SILENCE_TIMEOUT_MS}ms`
      );
    }, PEER_SILENCE_TIMEOUT_MS);
    parser.onAnyFrame = () => {
      clearTimeout(timer);
      parser.onAnyFrame = undefined;
    };
  }

  /**
   * 把一条「看着还开着、实际已经打不开流」的连接摘出连接池。
   *
   * newStream 失败（协议协商超时、muxer 拒绝）时，连接的 status 往往仍是 'open'
   * 且 timeline.close 为空，于是 acquireConnection 会把同一条死连接一直发给后续
   * 每一次调用——表现为「一直拉不下来、点刷新也没用，退出页面重进（换了 libp2p
   * 节点）才恢复」。
   *
   * 关闭走和 GOAWAY 完全一样的排空路径：只做标记 + 摘池，真正的 close 交给最后
   * 一条流结束时的 closeConnectionIfDrained，避免拆掉复用同一连接的兄弟流。
   */
  private evictConnection(
    connection: Connection | null,
    state: ConnectionState | null,
    reason: string
  ) {
    if (!connection) return;
    this.dropPooledConnection(connection);
    if (!state || state.evicted) return;
    state.evicted = true;
    console.warn(`[grpc] evicting pooled connection: ${reason}`);
    // 不动 waiters：它们等的是这条连接上的槽位，槽位一空照样可以试，失败也会各自
    // 抛错。开流失败也可能只是 maxOutboundStreams 打满（连接本身好着），一并拒掉
    // 会把本来能跑完的排队调用全部误杀。
    this.closeConnectionIfDrained(connection, state);
  }

  private notifyStreamSlotAvailable(state: ConnectionState) {
    if (state.waiters.length === 0) {
      return;
    }
    while (
      state.waiters.length > 0 &&
      state.activeStreams < state.maxConcurrentStreams
    ) {
      const waiter = state.waiters.shift();
      if (!waiter) break;
      try {
        waiter.resolve();
      } catch (err) {
        console.error("Error resolving stream waiter:", err);
      }
    }
  }

  /** 归还一条流占用的槽位，并在连接已排空时真正关闭它。 */
  private releaseStreamSlot(
    connection: Connection | null,
    state: ConnectionState | null
  ) {
    if (!state) return;
    state.activeStreams = Math.max(0, state.activeStreams - 1);
    this.notifyStreamSlotAvailable(state);
    this.closeConnectionIfDrained(connection, state);
  }

  /**
   * 取连接 + 排槽位 + 开流，开流失败时最多重拨一次。
   *
   * evictConnection 只保证「下一次调用」不再拿到这条死连接，本次仍旧抛错。但池里
   * 的连接随时可能在浏览器挂起、节点重启、GOAWAY 排空的空档里 muxer 先关掉，而
   * connection.status（取自 maConn）还是 'open'——用户侧就表现为「生成到一半忽然
   * 报 The connection muxer is "closing"」，紧接着手动重来一次又好了。此处请求一个
   * 字节都还没写出去（HTTP/2 头都在开流之后才发），重开一条流不存在重复提交的
   * 副作用。
   *
   * 只在「连接确实已经死了」时重试，且必须先 discardDeadConnection：dial 命中既有
   * 连接只看 status 不看 muxer，不 abort 掉重拨拿回来的还是同一条僵尸。连接仍然
   * 健康时开流失败（打满 maxOutboundStreams、协商超时）再拨一次只是把同一个错误
   * 推迟一个 dialTimeout，直接抛错。
   */
  private async openCallStream(
    tag: string,
    timeout: number,
    signal: AbortSignal | undefined,
    forceNew: boolean
  ): Promise<{
    connection: Connection;
    state: ConnectionState;
    stream: Stream;
  }> {
    let retried = false;
    for (;;) {
      if (signal?.aborted) {
        const reason = signal.reason;
        throw reason instanceof Error ? reason : new Error("Operation aborted");
      }
      const connection = await this.acquireConnection(forceNew, signal);
      const state = this.getConnectionState(connection as object);
      state.pendingStreams += 1;
      try {
        await this.waitForStreamSlot(state, signal, timeout);
        if (signal?.aborted) {
          const reason = signal.reason;
          throw reason instanceof Error
            ? reason
            : new Error("Operation aborted while opening HTTP/2 stream");
        }
        state.activeStreams += 1;
      } catch (err) {
        if (signal?.aborted) {
          throw err;
        }
        console.warn(`[${tag}] waiting for stream slot failed:`, err);
        // 等满整个调用超时都排不到槽位，说明这条连接上的流只进不出（半开流泄漏
        // 或对端不再放行），再排下去只是继续超时。摘池让下一次调用另拨一条。
        this.evictConnection(
          connection,
          state,
          `${tag} stream slot unavailable: ${
            err instanceof Error ? err.message : String(err)
          }`
        );
        throw err;
      } finally {
        state.pendingStreams -= 1;
      }
      // 不传 signal 时 libp2p 会自己挂一个 AbortSignal.timeout(协议协商超时) 到流上，
      // 且 negotiateFully:false 下开流立即返回、这个信号却继续生效：对端首字节稍慢
      // （节点在跑构建、模型排队）满 10 秒就把一条正常的流 abort 成
      // "signal timed out"。换成自有 controller，开流成功即 clearTimeout。
      // ⚠️ abort 原因必须与 AbortSignal.timeout() 完全一致："signal timed out"
      // 子串是上游用来把它归类成可重试连接问题的依据，各包独立发版，改文案会在
      // 版本错配时被误判成应用错误。
      const openController = createAbortController();
      const openTimer = setTimeout(
        () =>
          openController.abort(
            createAbortError("signal timed out", "TimeoutError")
          ),
        STREAM_OPEN_TIMEOUT_MS
      );
      const abortOpen = () => {
        const reason = signal?.reason;
        openController.abort(
          reason instanceof Error ? reason : new Error("Operation aborted")
        );
      };
      if (signal) {
        if (signal.aborted) {
          abortOpen();
        } else {
          signal.addEventListener("abort", abortOpen, { once: true });
        }
      }
      let stream: Stream;
      try {
        stream = await connection.newStream(this.protocol, {
          maxOutboundStreams: 50,
          signal: openController.signal,
          negotiateFully: false,
        });
      } catch (err) {
        // 开流失败的连接不会被 isConnectionReusable 判出来，不摘池就会被后续每一次
        // 调用继续复用，一直超时到用户重进页面为止。
        const reason = `${tag} newStream failed: ${
          err instanceof Error ? err.message : String(err)
        }`;
        // 复查一次：开流失败之后连接才露出马脚（muxer 已关、maConn 刚断），这时
        // 它是「拿到手里才死的」，本次调用一个字节都还没写出去，重开一条流没有
        // 重复提交的副作用。`signal timed out` 同样属于这个路径：协议协商超时后
        // status 可能还保持 open，若不主动丢弃，下一次 dial 会复用同一条连接。
        // 有其它活跃流时不能据此中止整条连接，等待上层重试即可，避免误伤并行请求。
        const connectionDied = !isConnectionReusable(connection);
        const streamOpenTimedOut = isStreamOpenTimeout(err);
        const hasSiblingStreams =
          state.activeStreams > 1 ||
          state.pendingStreams > 0 ||
          state.waiters.length > 0;
        const canReplaceConnection =
          connectionDied || (streamOpenTimedOut && !hasSiblingStreams);
        if (canReplaceConnection) {
          // libp2p 的 dial 只看 Connection.status；必须先 abort，才能保证下一轮
          // 真的创建新连接，而不是再次取得这个 status 仍为 open 的僵尸连接。
          discardDeadConnection(connection, reason);
        }
        this.releaseStreamSlot(connection, state);
        this.evictConnection(connection, state, reason);
        if (retried || !canReplaceConnection || signal?.aborted) {
          throw err;
        }
        retried = true;
        console.warn(
          `[${tag}] retrying on a freshly dialed connection after newStream failure`
        );
        continue;
      } finally {
        clearTimeout(openTimer);
        if (signal) signal.removeEventListener("abort", abortOpen);
      }
      return { connection, state, stream };
    }
  }

  private rejectStreamWaiters(state: ConnectionState, error: Error) {
    if (state.waiters.length === 0) {
      return;
    }
    const waiters = state.waiters.splice(0);
    for (const waiter of waiters) {
      try {
        waiter.reject(error);
      } catch (err) {
        console.error("Error rejecting stream waiter:", err);
      }
    }
  }

  private async waitForStreamSlot(
    state: ConnectionState,
    signal?: AbortSignal,
    timeoutMs: number = dialTimeout
  ): Promise<void> {
    if (state.maxConcurrentStreams <= 0) {
      throw new Error(
        "No available HTTP/2 streams: server advertised zero concurrent streams"
      );
    }
    if (
      !Number.isFinite(state.maxConcurrentStreams) ||
      state.activeStreams < state.maxConcurrentStreams
    ) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      // eslint-disable-next-line prefer-const
      let timeoutId: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        const idx = state.waiters.indexOf(waiter);
        if (idx >= 0) {
          state.waiters.splice(idx, 1);
        }
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        if (signal) {
          signal.removeEventListener("abort", onAbort);
        }
      };
      const waiter = {
        resolve: () => {
          if (settled) return;
          settled = true;
          cleanup();
          resolve();
        },
        reject: (err: Error) => {
          if (settled) return;
          settled = true;
          cleanup();
          reject(err);
        },
      };
      const onAbort = () => {
        waiter.reject(
          new Error("Aborted while waiting for available HTTP/2 stream slot")
        );
      };
      if (signal) {
        if (signal.aborted) {
          onAbort();
          return;
        }
        signal.addEventListener("abort", onAbort, { once: true });
      }
      timeoutId =
        timeoutMs > 0
          ? setTimeout(() => {
              waiter.reject(
                new Error("Timed out waiting for available HTTP/2 stream slot")
              );
            }, timeoutMs)
          : undefined;
      state.waiters.push(waiter);
    });
  }

  private async sendFrameWithFlowControl(
    parser: HTTP2Parser,
    streamId: number,
    frame: Uint8Array,
    writer: StreamWriter,
    signal?: AbortSignal,
    timeoutMs: number = DEFAULT_SEND_WINDOW_TIMEOUT
  ): Promise<void> {
    if (signal?.aborted) {
      throw new Error("Operation aborted");
    }
    const payloadLength = Math.max(0, frame.length - 9);
    if (payloadLength > 0) {
      const { conn, stream } = parser.getSendWindows(streamId);
      if (conn < payloadLength || stream < payloadLength) {
        console.debug(
          `[stream ${streamId}] waiting for send window: need=${payloadLength} conn=${conn} stream=${stream}`
        );
      }
      try {
        await parser.waitForSendWindow(streamId, payloadLength, timeoutMs);
      } catch (err) {
        console.warn(
          `[stream ${streamId}] send window wait failed (${
            (err as Error)?.message ?? err
          }); continuing best-effort`
        );
        const forcedCredit = Math.max(payloadLength, 256 << 10);
        parser.unsafeForceExtendSendWindow(streamId, forcedCredit);
      }
    }
    if (signal?.aborted) {
      throw new Error("Operation aborted");
    }
    await writer.write(frame);
    if (payloadLength > 0) {
      parser.consumeSendWindow(streamId, payloadLength);
    }
  }

  private async acquireConnection(
    forceNew: boolean,
    signal?: AbortSignal,
  ): Promise<Connection> {
    if (signal?.aborted) {
      throw getAbortError(signal);
    }
    const key = this.poolKey;
    if (!forceNew) {
      const pooled = sharedConnectionPool.get(key);
      if (pooled) {
        const conn = pooled.connection;
        if (conn) {
          if (isConnectionReusable(conn)) {
            return conn;
          }
          // 被 abort 的连接不派发 close 事件，靠监听剔除不掉。
          // 复用它会让后续每次调用都超时，表现为"断线后再也连不上"。
          console.warn(
            `[grpc] dropping unusable pooled connection (status=${
              conn.status ?? "unknown"
            })`
          );
          sharedConnectionPool.delete(key);
          void safeCloseConnection(conn);
        } else {
          if (!signal) return pooled.promise;
          return await new Promise<Connection>((resolve, reject) => {
            const onAbort = () => reject(getAbortError(signal));
            signal.addEventListener("abort", onAbort, { once: true });
            pooled.promise.then(
              (connection) => {
                signal.removeEventListener("abort", onAbort);
                resolve(connection);
              },
              (error) => {
                signal.removeEventListener("abort", onAbort);
                reject(error);
              },
            );
          });
        }
      }
    }

    const dialController = createAbortController();
    const dialTimer = setTimeout(
      () => dialController.abort(new Error("Dial timed out")),
      dialTimeout,
    );
    const abortDial = () => dialController.abort(getAbortError(signal));
    if (signal) signal.addEventListener("abort", abortDial, { once: true });

    const dialPromise = this.node
      .dial(this.peerAddr, { signal: dialController.signal })
      .then(async (conn) => {
        if (!forceNew) {
          const poolEntry = sharedConnectionPool.get(key);
          if (poolEntry && poolEntry.promise === dialPromise) {
            poolEntry.connection = conn;
            const removeFromPool = () => {
              const current = sharedConnectionPool.get(key);
              if (current && current.promise === dialPromise) {
                sharedConnectionPool.delete(key);
              }
            };
            try {
              // 注意：Connection.abort() 只向下传递给 muxer/maConn，不派发任何
              // 事件，所以 abort 掉的连接无法靠监听剔除，只能在取用时用
              // isConnectionReusable 主动判定。
              conn.addEventListener("close", removeFromPool, { once: true });
            } catch { /* ignore event listener registration errors */ }
          }
        }
        return conn;
      })
      .catch((err) => {
        if (!forceNew) {
          const pooled = sharedConnectionPool.get(key);
          if (pooled && pooled.promise === dialPromise) {
            sharedConnectionPool.delete(key);
          }
        }
        throw err;
      })
      .finally(() => {
        clearTimeout(dialTimer);
        if (signal) signal.removeEventListener("abort", abortDial);
      });

    if (!forceNew) {
      sharedConnectionPool.set(key, { promise: dialPromise });
    }

    return dialPromise;
  }

  private getDefaultTransportProfile(mode: CallMode): TransportProfile {
    switch (mode) {
      case "server-streaming":
        return "compatibility";
      case "client-streaming":
      case "bidirectional":
        return "flow-control";
      default:
        return "flow-control";
    }
  }

  setToken(token: string) {
    this.token = token;
  }

  /** 从 peerAddr 提取 HTTP/2 :authority 字段（host:port 格式） */
  private getAuthority(): string {
    try {
      const addr = this.peerAddr.toString();
      const ip4 = addr.match(/\/ip4\/(\d[\d.]+)\/tcp\/(\d+)/);
      if (ip4) return `${ip4[1]}:${ip4[2]}`;
      const ip6 = addr.match(/\/ip6\/([^/]+)\/tcp\/(\d+)/);
      if (ip6) return `[${ip6[1]}]:${ip6[2]}`;
      const dns = addr.match(/\/dns(?:4|6)?\/([.\w-]+)\/tcp\/(\d+)/);
      if (dns) return `${dns[1]}:${dns[2]}`;
    } catch { /* ignore */ }
    return 'localhost';
  }

  async unaryCall(
    method: string,
    requestData: Uint8Array,
    timeout: number = 30000,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    let stream: Stream | null = null;
    let responseData: Uint8Array | null = null;
    let responseBuffer: Uint8Array[] = []; // 添加缓冲区来累积数据
    let responseDataExpectedLength = -1; // 当前响应的期望长度
    /** 跨 DATA 帧的部分 gRPC 消息头缓冲（当一帧的 payload < 5 字节时积累） */
    let headerPartialBuffer: Uint8Array[] = [];
    const hpack = new HPACK();
    let exitFlag = false;
    let errMsg = "";
    let isResponseComplete = false; // 添加标志来标识响应是否完成
    /** 事件驱动：响应完成时的唤醒函数 */
    let notifyResponseComplete: (() => void) | null = null;
    let connection: Connection | null = null;
    let state: ConnectionState | null = null;
    let streamSlotAcquired = false;
    // 提升 writer 作用域到 finally 可访问，确保错误路径下也能调用 abort() 清理资源
    let writerRef: StreamWriter | null = null;
    let abortReject: ((error: Error) => void) | null = null;
    const abortError = (): Error => {
      const reason = signal?.reason;
      return reason instanceof Error ? reason : new Error("Operation aborted");
    };
    const abortPromise = signal
      ? new Promise<never>((_, reject) => {
          abortReject = reject;
        })
      : null;
    // 取消可以发生在开流或握手阶段；该拒绝稍后会被主流程抛出，先绑定兜底
    // handler 避免 Promise 尚未被 race 消费时触发 unhandled rejection。
    void abortPromise?.catch(() => undefined);
    const abortListener = () => {
      const error = abortError();
      try {
        writerRef?.abort("unaryCall cancelled");
      } catch {
        /* cleanup is best effort */
      }
      try {
        stream?.abort(error);
      } catch {
        /* cleanup is best effort */
      }
      abortReject?.(error);
    };
    if (signal) {
      if (signal.aborted) throw abortError();
      signal.addEventListener("abort", abortListener, { once: true });
    }
    try {
      // const stream = await this.node.dialProtocol(this.peerAddr, this.protocol)
      const opened = await this.openCallStream(
        "unaryCall",
        timeout,
        signal,
        false
      );
      connection = opened.connection;
      state = opened.state;
      streamSlotAcquired = true;
      stream = opened.stream;
      const streamManager = this.getStreamManagerFor(connection as object);
      const streamId = await streamManager.getNextAppLevelStreamId();
      const writer = new StreamWriter(stream, {
        bufferSize: 16 * 1024 * 1024,
      });
      writerRef = writer;
      try {
        writer.addEventListener("backpressure", (e: CustomEvent) => {
          const d = e.detail || {};
          console.warn(
            `[unary stream ${streamId}] backpressure current=${d.currentSize} avg=${d.averageSize} threshold=${d.threshold}`
          );
        });
        writer.addEventListener("drain", () => {
          // drain event - no action needed
        });
        writer.addEventListener("stalled", (e: CustomEvent) => {
          const d = e.detail || {};
          console.warn(
            `[unary stream ${streamId}] stalled queue=${d.queueSize} drained=${d.drained} since=${d.sinceMs}ms — sending PING`
          );
          try {
            const payload = new Uint8Array(8);
            crypto.getRandomValues?.(payload);
            const ping = Http2Frame.createFrame(0x6, 0x0, 0, payload);
            writer.write(ping);
          } catch { /* ignore ping write errors */ }
        });
      } catch { /* ignore addEventListener errors */ }
      const parser = new HTTP2Parser(writer);
      this.armPeerSilenceWatchdog(parser, connection, state, "unaryCall");
      parser.onGoaway = (info) => {
        console.warn("[unaryCall] GOAWAY received from server", info);
        this.dropPooledConnection(connection!);
        if (state) {
          this.rejectStreamWaiters(
            state,
            new Error("Connection received GOAWAY")
          );
        }
        exitFlag = true;
        errMsg = `GOAWAY received: code=${info.errorCode}`;
        notifyResponseComplete?.(); // 唤醒等待中的 Promise
        // 不在此处关闭连接：GOAWAY 只终结本次调用自己的流，连接上可能还有
        // 其他并发调用的流在跑。标记后由最后一条流在 finally 中排空关闭。
        if (state) {
          state.goawayReceived = true;
        }
      };
      parser.onSettingsParsed = (settings) => {
        if (
          state &&
          settings.maxConcurrentStreams !== undefined &&
          settings.maxConcurrentStreams > 0
        ) {
          state.maxConcurrentStreams = settings.maxConcurrentStreams;
          this.notifyStreamSlotAvailable(state);
        }
      };
      parser.registerOutboundStream(streamId);
      responseDataExpectedLength = -1; // 重置期望长度
      responseBuffer = []; // 重置缓冲区
      headerPartialBuffer = []; // 重置跨帧头部缓冲
      parser.onData = (payload, frameHeader)  => {
        //接收数据
        if (responseDataExpectedLength === -1) {
          //grpc消息头部未读取
          // 如果有跨帧积累的部分头字节，先与本帧 payload 合并
          let effectivePayload = payload;
          if (headerPartialBuffer.length > 0) {
            headerPartialBuffer.push(payload);
            const totalLen = headerPartialBuffer.reduce((s, c) => s + c.length, 0);
            effectivePayload = new Uint8Array(totalLen);
            let off = 0;
            for (const c of headerPartialBuffer) { effectivePayload.set(c, off); off += c.length; }
            headerPartialBuffer = [];
          }
          //提取gRPC消息头部
          if (effectivePayload.length < 5) {
            // 头部字节不足 5，先缓存，等待后续帧补全
            headerPartialBuffer.push(effectivePayload);
            return;
          }
          const lengthBytes = effectivePayload.slice(1, 5); // 消息长度的4字节
          responseDataExpectedLength = new DataView(
            lengthBytes.buffer,
            lengthBytes.byteOffset
          ).getUint32(0, false); // big-endian（getUint32 返回无符号整数，结果不会为负）
          if (responseDataExpectedLength + 5 > effectivePayload.length) {
            // 如果当前 payload 不足以包含完整的 gRPC 消息，缓存数据
            const grpcData = effectivePayload.subarray(5);
            responseBuffer.push(grpcData);
            responseDataExpectedLength -= grpcData.length; // 更新期望长度
            return;
          } else {
            // payload 已包含完整的 gRPC 消息体，精确截取（避免尾部多余字节污染）
            const msgLen = responseDataExpectedLength;
            const grpcData = effectivePayload.slice(5, 5 + msgLen);
            responseBuffer.push(grpcData);
            responseData = grpcData;
            isResponseComplete = true;
            responseDataExpectedLength = -1;
            notifyResponseComplete?.();
          }
        } else if (responseDataExpectedLength > 0) {
          //grpc消息头部已读取
          responseDataExpectedLength -= payload.length;
          if (responseDataExpectedLength <= 0) {
            // 超收时截掉多余字节
            const exactPayload = responseDataExpectedLength < 0
              ? payload.slice(0, payload.length + responseDataExpectedLength)
              : payload;
            responseBuffer.push(exactPayload);
            responseData = new Uint8Array(
              responseBuffer.reduce((sum, chunk) => sum + chunk.length, 0)
            );
            let offset = 0;
            for (const chunk of responseBuffer) {
              responseData.set(chunk, offset);
              offset += chunk.length;
            }
            responseDataExpectedLength = -1;
            isResponseComplete = true;
            notifyResponseComplete?.();
          } else {
            responseBuffer.push(payload); // 还不完整，继续累积
          }
        }
        // END_STREAM 兜底：数据路径已处理大多数情况；此分支仅在边缘情况下触发
        if (frameHeader && frameHeader.flags & 0x1 && !isResponseComplete) {
          if (responseBuffer.length > 0) {
            const totalLength = responseBuffer.reduce((sum, c) => sum + c.length, 0);
            responseData = new Uint8Array(totalLength);
            let offset = 0;
            for (const chunk of responseBuffer) { responseData.set(chunk, offset); offset += chunk.length; }
          } else {
            responseData = new Uint8Array(0);
          }
          isResponseComplete = true;
          notifyResponseComplete?.();
        }
      };
      parser.onEnd = () => {
        // 流结束时若响应未标记完成（空响应 / 纯 trailers），强制标记并唤醒等待者
        if (!isResponseComplete) {
          isResponseComplete = true;
          notifyResponseComplete?.();
        }
      };
      parser.onSettings = () => {
        //接收settings,反馈ack
        const ackSettingFrame = Http2Frame.createSettingsAckFrame();
        writer.write(ackSettingFrame);
      };
      parser.onHeaders = (headers) => {
        const plainHeaders = hpack.decodeHeaderFields(headers);
        if (plainHeaders.get("grpc-status") === "0") {
          // 成功状态
        } else if (plainHeaders.get("grpc-status") !== undefined) {
          exitFlag = true;
          errMsg = decodeGrpcMessage(plainHeaders.get("grpc-message") || "") || "gRPC call failed";
          notifyResponseComplete?.(); // 唤醒等待中的 Promise
        }
      };
      // 启动后台流处理，捕获任何异步错误
      parser.processStream(stream).catch((error: unknown) => {
        // 若响应已完整收到（isResponseComplete=true），后置的网络层错误属于正常的
        // 连接拆除过程（如服务端 RST、连接关闭），不影响已成功的调用结果，静默忽略。
        // 若响应尚未完成，才记录错误并唤醒等待者，触发超时/错误路径。
        if (!isResponseComplete) {
          console.error('Error in processStream:', error);
          exitFlag = true;
          if (!errMsg) {
            errMsg = error instanceof Error ? error.message : 'Stream processing failed';
          }
          notifyResponseComplete?.(); // 流处理异常也需唤醒等待者
        }
      });
      
      // 握手
      const preface = Http2Frame.createPreface();
      await writer.write(preface);
      // 发送Settings请求
      const settingFrme = Http2Frame.createSettingsFrame();
      await writer.write(settingFrme);
      // 等待对端 SETTINGS 或 ACK，择一即可，避免偶发握手竞态
      // 注意：未胜出的 promise 内部有超时定时器，它们最终会 reject。
      // 必须绑定 .catch(…) 消除错误，否则在 Node.js 新版本中会导致 UnhandledPromiseRejection 崩溃。
      await Promise.race([
        parser.waitForPeerSettings(1000).catch(() => {}),
        parser.waitForSettingsAck().catch(() => {}),
        new Promise<void>((res) => setTimeout(res, 300)),
      ]);
      // 即使未等到，也继续；多数实现会随后发送
      // 创建头部帧
      const headerFrame = Http2Frame.createHeadersFrame(
        streamId,
        method,
        true,
        this.token,
        this.getAuthority()
      );
      await writer.write(headerFrame);
      // 直接按帧大小分片发送（保持与之前一致的稳定路径）
      const dataFrames = Http2Frame.createDataFrames(
        streamId,
        requestData,
        true
      );
      const frameSendTimeout =
        timeout > 0 ? timeout : DEFAULT_SEND_WINDOW_TIMEOUT;
      for (const df of dataFrames) {
        await this.sendFrameWithFlowControl(
          parser,
          streamId,
          df,
          writer,
          signal,
          frameSendTimeout
        );
      }
      // 等待 responseData 不为空，或超时（事件驱动，不轮询）
      const responseWait = new Promise<void>((resolve, reject) => {
        if (isResponseComplete || exitFlag) { resolve(); return; }
        const t = setTimeout(() => {
          notifyResponseComplete = null;
          reject(new Error("gRPC response timeout"));
        }, timeout);
        notifyResponseComplete = () => {
          clearTimeout(t);
          notifyResponseComplete = null;
          resolve();
        };
      });
      if (abortPromise) {
        await Promise.race([responseWait, abortPromise]);
      } else {
        await responseWait;
      }
      try {
        await writer.flush(timeout);
      } catch { /* ignore flush errors */ }
      await writer.end();
    } catch (err) {
      console.error("unaryCall error:", err);
      throw err;
    } finally {
      // 必须先 abort writer（立即强制停止 pushable + stream），再 close stream。
      // 若顺序颠倒：stream.close() 会等待服务端半关闭确认，网络异常时永久挂住，
      // 导致 writer.abort() 永远不执行 → watchdog 定时器 / pushable 泄漏。
      // writer.abort() 内部幂等，成功路径下 writer.end() 已调用 cleanup()，安全。
      writerRef?.abort('unaryCall cleanup');
      if (stream) {
        // close() 超时会强制 abort，避免半开流长期占用 maxOutboundStreams 名额。
        await closeStreamWithTimeout(stream);
      }
      if (streamSlotAcquired && state) {
        state.activeStreams = Math.max(0, state.activeStreams - 1);
        this.notifyStreamSlotAvailable(state);
      }
      // 本流已结束：若连接收到过 GOAWAY 且已无活跃流，此时才真正关闭。
      this.closeConnectionIfDrained(connection, state);
      if (signal) signal.removeEventListener("abort", abortListener);
      abortReject = null;
    }
    if (exitFlag) {
      throw new Error(errMsg);
    }
    if (!responseData) {
      responseData = new Uint8Array();
    }
    return responseData;
  }

  /**
   * 执行GRPC调用，支持通过context和返回的取消函数控制终止
   * @param method GRPC方法名
   * @param requestData 请求数据
   * @param timeout 超时时间(毫秒)
   * @param mode 调用模式: 'unary'|'server-streaming'|'client-streaming'|'bidirectional'
   * @param onDataCallback 数据回调函数
   * @param dataSourceCallback 客户端流数据源回调，支持单个chunk或批量chunks（使用frames策略优化）
   * @param onEndCallback 结束回调函数
   * @param onErrorCallback 错误回调函数
   * @param context 操作上下文，包含AbortSignal用于取消操作
   * @param options 调用选项（可配置批处理、连接行为以及传输策略）
   * @param options.transportProfile 传输策略: 'flow-control'（默认，适合上传/双向流）或 'compatibility'（兼容旧逻辑，适合高并发 server-streaming）
   * @returns 取消函数，可随时调用终止操作
   */
  async Call(
    method: string,
    requestData: Uint8Array,
    timeout: number = 30000,
    mode: CallMode,
    onDataCallback: (payload: Uint8Array) => void,
    dataSourceCallback?: () => AsyncIterable<Uint8Array | Uint8Array[]>,
    onEndCallback?: () => void,
    onErrorCallback?: (error: unknown) => void,
    context?: { signal?: AbortSignal },
    options?: CallOptions
  ) {
    // 创建内部AbortController用于控制操作
    const internalController = createAbortController();
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    let stream: Stream | null = null;
    // 保存外部 abort 监听器引用，以便操作结束后移除，防止内存泄漏
    let contextAbortHandler: (() => void) | undefined;

    const profile: TransportProfile =
      options?.transportProfile ?? this.getDefaultTransportProfile(mode);
    const useFlowControl = profile === "flow-control";

    // 取消函数 - 将在最后返回给调用者
    const cancelOperation = () => {
      internalController.abort();
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
      if (stream) {
        try {
          // Cancellation must stop the old stream immediately. A detached
          // close() promise can reject with StreamAbortEvent and leaves the
          // previous call alive while the caller starts its retry.
          stream.abort(new Error("Operation cancelled"));
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          if (!/clos(?:ed|ing)|abort/i.test(message)) {
            console.warn("Error aborting stream on cancel:", err);
          }
        }
      }
    };

    // 如果提供了外部信号，监听它
    if (context?.signal) {
      // 如果外部信号已经触发中止，立即返回——避免启动 IIFE 后在 catch 中再次调用 onErrorCallback
      if (context.signal.aborted) {
        if (onErrorCallback) {
          onErrorCallback(new Error("Operation aborted by context"));
        }
        return cancelOperation;
      }

      // 监听外部的abort事件（保存引用以便后续移除，防止内存泄漏）
      contextAbortHandler = () => { cancelOperation(); };
      context.signal.addEventListener("abort", contextAbortHandler);
    }

    // 超时Promise
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error("Operation timed out"));
        cancelOperation();
      }, timeout);
    });

    // 主操作Promise
    const operationPromise = (async () => {
      /**
       * 统一错误报告：确保 onErrorCallback 只被调用一次，
       * 并同时中止操作，防止后续再触发 onEndCallback。
       * 适用于 onGoaway / onHeaders / processStream.catch / onData 等各个错误路径。
       */
      let errorCallbackFired = false;
      const reportError = (err: unknown) => {
        if (errorCallbackFired) return;
        errorCallbackFired = true;
        internalController.abort();
        if (onErrorCallback) onErrorCallback(err);
      };

      /** 分段列表缓冲，避免每次 payload 到达时 O(n) 全量拷贝 */
      let msgChunks: Uint8Array[] = [];
      let msgTotalLen = 0;
      let expectedMessageLength = -1; // 当前消息的期望长度
      /** 将分段列表合并为单一 Uint8Array（仅在需要时调用） */
      const flattenMsgBuffer = (): Uint8Array => {
        if (msgChunks.length === 0) return new Uint8Array(0);
        if (msgChunks.length === 1) return msgChunks[0];
        const out = new Uint8Array(msgTotalLen);
        let off = 0;
        for (const c of msgChunks) { out.set(c, off); off += c.length; }
        return out;
      };
      const hpack = new HPACK();
      let connection: Connection | null = null;
      let state: ConnectionState | null = null;
      let streamSlotAcquired = false;
      // 提升 writer 作用域到 finally 可访问，确保 unary/server-streaming 模式下也能清理资源
      let writer: StreamWriter | null = null;

      try {
        // 检查是否已经中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // 如开启 freshConnection，则在拨号前尝试断开现有连接，确保本次使用全新连接（注意：会影响与该节点的其他并发）
        if (options?.freshConnection) {
          try {
            sharedConnectionPool.delete(this.poolKey);
            await this.node.hangUp(this.peerAddr);
            console.warn(
              "[Call] hangUp existing connection before dialing due to freshConnection=true"
            );
          } catch (err) {
            console.warn(
              "[Call] hangUp failed or not supported, proceeding to dial",
              err
            );
          }
        }

        const opened = await this.openCallStream(
          "Call",
          timeout,
          internalController.signal,
          Boolean(options?.freshConnection)
        );
        connection = opened.connection;
        state = opened.state;
        streamSlotAcquired = true;
        stream = opened.stream;
        const streamManager = this.getStreamManagerFor(connection as object);
        const streamId = await streamManager.getNextAppLevelStreamId();
        writer = new StreamWriter(stream, {
          bufferSize: 16 * 1024 * 1024,
        });
        try {
          writer.addEventListener("backpressure", (e: CustomEvent) => {
            const d = e.detail || {};
            console.warn(
              `[stream ${streamId}] backpressure current=${d.currentSize} avg=${d.averageSize} threshold=${d.threshold}`
            );
          });
          writer.addEventListener("drain", () => {
            // drain event - no action needed
          });
          writer.addEventListener("stalled", (e: CustomEvent) => {
            const d = e.detail || {};
            console.warn(
              `[stream ${streamId}] stalled queue=${d.queueSize} drained=${d.drained} since=${d.sinceMs}ms — sending PING`
            );
            try {
              const payload = new Uint8Array(8);
              crypto.getRandomValues?.(payload);
              const ping = Http2Frame.createFrame(0x6, 0x0, 0, payload);
              writer!.write(ping);
            } catch { /* ignore ping write errors */ }
          });
        } catch { /* ignore addEventListener errors */ }
        const parser = new HTTP2Parser(writer!, {
          compatibilityMode: !useFlowControl,
        });
        this.armPeerSilenceWatchdog(parser, connection, state, "Call");
        parser.onGoaway = (info) => {
          console.warn("[Call] GOAWAY received from server", info);
          if (connection) {
            this.dropPooledConnection(connection);
          }
          if (state) {
            this.rejectStreamWaiters(
              state,
              new Error("Connection received GOAWAY")
            );
          }
          // reportError 统一完成：标记已报错 + abort + 触发回调（幂等，不会重复触发）
          reportError(new Error(`GOAWAY received: code=${info.errorCode}`));
          // 不在此处关闭连接：GOAWAY 只终结本次调用自己的流，连接上可能还有
          // 其他并发调用的流在跑（并行生成时就是这种情形）。标记后由最后一条
          // 流在 finally 中排空关闭。
          if (state) {
            state.goawayReceived = true;
          }
        };
        parser.onSettingsParsed = (settings) => {
          if (
            state &&
            settings.maxConcurrentStreams !== undefined &&
            settings.maxConcurrentStreams > 0
          ) {
            state.maxConcurrentStreams = settings.maxConcurrentStreams;
            this.notifyStreamSlotAvailable(state);
          }
        };
        if (useFlowControl) {
          parser.registerOutboundStream(streamId);
        }
        const sendWindowTimeout =
          timeout > 0 ? timeout : DEFAULT_SEND_WINDOW_TIMEOUT;
        const writeFrame = async (frame: Uint8Array) => {
          if (useFlowControl) {
            await this.sendFrameWithFlowControl(
              parser,
              streamId,
              frame,
              writer!,
              internalController.signal,
              sendWindowTimeout
            );
          } else {
            if (internalController.signal.aborted) {
              throw new Error("Operation aborted");
            }
            await writer!.write(frame);
          }
        };
        const writeDataFrames = async (frames: Uint8Array[]) => {
          for (const frame of frames) {
            await writeFrame(frame);
          }
        };

        // 在各个回调中检查是否已中止
        parser.onData = async (payload): Promise<void> => {
          if (internalController.signal.aborted) return;

          try {
            // 追加到分段列表，O(1)，不拷贝历史数据
            msgChunks.push(payload);
            msgTotalLen += payload.length;

            // 处理缓冲区中的完整消息
            while (msgTotalLen > 0) {
              if (internalController.signal.aborted) return;

              // 读取 gRPC 消息头（5字节）
              if (expectedMessageLength === -1 && msgTotalLen >= 5) {
                const flat = flattenMsgBuffer();
                msgChunks = [flat];
                const lengthBytes = flat.slice(1, 5);
                expectedMessageLength = new DataView(
                  lengthBytes.buffer,
                  lengthBytes.byteOffset
                ).getUint32(0, false);
              }

              // 有完整消息
              if (expectedMessageLength !== -1 && msgTotalLen >= expectedMessageLength + 5) {
                const flat = flattenMsgBuffer();
                msgChunks = [flat];
                const completeMessage = flat.slice(5, expectedMessageLength + 5);
                onDataCallback(completeMessage);
                // 移除已处理消息，保留剩余
                const remaining = flat.slice(expectedMessageLength + 5);
                msgChunks = remaining.length > 0 ? [remaining] : [];
                msgTotalLen = remaining.length;
                expectedMessageLength = -1;
              } else {
                break;
              }
            }
          } catch (error: unknown) {
            // reportError 统一报错并中止，防止 onEndCallback 在数据处理异常后仍被调用
            reportError(error);
          }
        };

        parser.onSettings = () => {
          // 检查是否已中止
          if (internalController.signal.aborted) return;

          const ackSettingFrame = Http2Frame.createSettingsAckFrame();
          writer!.write(ackSettingFrame);
        };

        parser.onHeaders = (headers) => {
          // 检查是否已中止
          if (internalController.signal.aborted) return;

          const plainHeaders = hpack.decodeHeaderFields(headers);
          if (plainHeaders.get("grpc-status") === "0") {
            // 成功状态
          } else if (plainHeaders.get("grpc-status") !== undefined) {
            const errMsg =
              decodeGrpcMessage(plainHeaders.get("grpc-message") || "") ||
              "gRPC call failed";
            // reportError 统一完成：标记已报错 + abort + 触发回调（幂等，不会重复触发）
            reportError(new Error(errMsg));
          }
        };
      // 启动后台流处理
      parser.processStream(stream).catch((error: unknown) => {
        // abort() 触发的清理错误属于预期行为，不打印错误日志，不重复触发回调
        if (!internalController.signal.aborted) {
          console.error('Error in processStream:', error);
          reportError(error);
        }
      });

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // Handshake - send HTTP/2 preface
        const preface = Http2Frame.createPreface();
        await writer.write(preface);

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // Send Settings request
        const settingFrame = Http2Frame.createSettingsFrame();
        await writer.write(settingFrame);

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // 等待对端 SETTINGS 或 ACK，择一即可，避免偶发握手竞态
        // 注意：未胜出的 promise 内部有超时定时器，它们最终会 reject。
        // 必须绑定 .catch(…) 消除错误，否则在 Node.js 新版本中会导致 UnhandledPromiseRejection 崩溃。
        {
          await Promise.race([
            parser.waitForPeerSettings(1000).catch(() => {}),
            parser.waitForSettingsAck().catch(() => {}),
            new Promise<void>((res) => setTimeout(res, 300)),
          ]);
          // 即使未等到，也继续；多数实现会随后发送
        }

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // Create header frame
        const headerFrame = Http2Frame.createHeadersFrame(
          streamId,
          method,
          true,
          this.token,
          this.getAuthority()
        );
        if (mode === "unary" || mode === "server-streaming") {
          await writer.write(headerFrame);
          const dfs = Http2Frame.createDataFrames(streamId, requestData, true);
          await writeDataFrames(dfs);

          // 检查是否已中止
          if (internalController.signal.aborted) {
            throw new Error("Operation aborted");
          }
        } else if (
          (mode === "client-streaming" || mode === "bidirectional") &&
          dataSourceCallback
        ) {
          await writer.write(headerFrame);

          // 检查是否已中止
          if (internalController.signal.aborted) {
            throw new Error("Operation aborted");
          }

          if (requestData.length > 0) {
            const dfs0 = Http2Frame.createDataFrames(
              streamId,
              requestData,
              false
            );
            await writeDataFrames(dfs0);
          }

          // 动态批量处理逻辑 - 在处理过程中动态补充新数据
          const batchSize = options?.batchSize || 10;

          // 动态批处理器
          const processingQueue: {
            chunk: Uint8Array;
            resolve: (value: void | PromiseLike<void>) => void;
            reject: (reason?: unknown) => void;
          }[] = [];

          /** 事件驱动：批处理完成后唤醒 waitForQueue 等待者 */
          const batchDoneWaiters: Array<() => void> = [];

          let isProcessing = false;

          const _notifyBatchDone = () => {
            const ws = batchDoneWaiters.splice(0);
            for (const fn of ws) { try { fn(); } catch { /* ignore */ } }
          };

          const processNextBatch = async () => {
            if (isProcessing || processingQueue.length === 0) return;
            isProcessing = true;

            let currentBatch: typeof processingQueue = [];

            try {
              // 收集当前批次的数据
              currentBatch = processingQueue.splice(
                0,
                Math.min(batchSize, processingQueue.length)
              );

              if (currentBatch.length > 1) {
                // 批量处理：为每个chunk创建HTTP/2帧
                // 顺序按窗口发送每个 chunk（避免跨流窗口互相干扰）
                for (const item of currentBatch) {
                  if (internalController.signal.aborted)
                    throw new Error("Operation aborted");
                  const frames = Http2Frame.createDataFrames(
                    streamId,
                    item.chunk,
                    false
                  );
                  await writeDataFrames(frames);
                }

                // 通知所有chunk处理完成
                currentBatch.forEach((item) => item.resolve());
              } else if (currentBatch.length === 1) {
                // 单个chunk处理
                const item = currentBatch[0];
                const frames1 = Http2Frame.createDataFrames(
                  streamId,
                  item.chunk,
                  false
                );
                await writeDataFrames(frames1);
                item.resolve();
              }
            } catch (error) {
              // 处理错误，通知当前批次的所有chunk处理失败
              currentBatch.forEach((item) => {
                try {
                  item.reject(error);
                } catch (err) {
                  // 忽略 reject 可能的错误（Promise 已经被处理）
                  console.warn("Error rejecting promise:", err);
                }
              });
            } finally {
              isProcessing = false;

              // 如果队列中还有数据，继续处理
              if (processingQueue.length > 0 && !internalController.signal.aborted) {
                // 直接递归调用（已是 async，自动让出事件循环）
                processNextBatch().catch((err) => { console.error("Error in processNextBatch:", err); });
              } else {
                // 队列清空，唤醒等待者
                _notifyBatchDone();
              }
            }
          };

          const addToQueue = (chunk: Uint8Array): Promise<void> => {
            return new Promise((resolve, reject) => {
              // 检查是否已经取消
              if (internalController.signal.aborted) {
                reject(new Error("Operation aborted"));
                return;
              }

              processingQueue.push({ chunk, resolve, reject });

              // 如果队列达到批量大小或没有在处理，立即开始处理
              if (processingQueue.length >= batchSize || !isProcessing) {
                processNextBatch().catch((err) => {
                  console.error("Error in processNextBatch:", err);
                });
              }
            });
          };

          // 处理数据源
          try {
            for await (const chunkOrChunks of dataSourceCallback()) {
              // 检查是否已中止
              if (internalController.signal.aborted) {
                throw new Error("Operation aborted");
              }

              // 处理单个chunk或批量chunks
              const chunksToProcess: Uint8Array[] = Array.isArray(chunkOrChunks)
                ? chunkOrChunks
                : [chunkOrChunks];

              // 将所有chunks添加到动态处理队列
              const addPromises = chunksToProcess.map((chunk) =>
                addToQueue(chunk)
              );

              // 等待当前批次的chunks被添加到队列（不等待处理完成）
              await Promise.all(addPromises);
            }
          } catch (error) {
            // 取消所有待处理的Promise
            const remainingQueue = processingQueue.splice(0);
            remainingQueue.forEach((item) => {
              try {
                item.reject(error);
              } catch (err) {
                console.warn("Error rejecting remaining promise:", err);
              }
            });
            throw error;
          }

          // 等待所有剩余的数据处理完成（事件驱动，无 10ms 轮询）
          await new Promise<void>((resolve, reject) => {
            const check = () => {
              if (internalController.signal.aborted) {
                reject(new Error("Operation aborted"));
                return;
              }
              if (processingQueue.length === 0 && !isProcessing) {
                resolve();
                return;
              }
              // processNextBatch 结束时会通知这里
              batchDoneWaiters.push(check);
            };
            check();
          });

          // 检查是否已中止
          if (internalController.signal.aborted) {
            throw new Error("Operation aborted");
          }

          // 发送纯 HTTP/2 END_STREAM 信号帧（0 字节 payload），而非带 gRPC 消息头的空消息。
          // createDataFrame 会额外附加 5 字节 gRPC 消息头 [0,0,0,0,0]，服务端会将其解析
          // 为一个长度=0 的额外 gRPC 消息，而不仅仅是流结束信号，可能导致协议混淆。
          const finalFrame = Http2Frame.createFrame(0x0, 0x01, streamId, new Uint8Array(0));
          await writeFrame(finalFrame);
          // 在结束前尽量冲刷内部队列，避免服务器看到部分数据 + context canceled
          try {
            await writer.flush(timeout);
          } catch { /* ignore flush errors */ }
          await writer.end();
        }

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // 仅在未中止时等待并回调：
        // 1. 若已中止（如 onHeaders gRPC 错误），跳过 waitForEndOfStream(0) 避免永久阻塞
        //    （waitForEndOfStream(0) 无超时，需等到 processStream 自然结束，
        //     而 processStream 结束依赖 stream.close()，但 stream.close() 在 finally 中——形成死锁）
        // 2. 避免在 onErrorCallback 之后再调用 onEndCallback
        if (!internalController.signal.aborted) {
          await parser.waitForEndOfStream(0);
          // Yield one microtask tick so that processStream.catch (which calls
          // reportError + internalController.abort()) has a chance to run before
          // we check abort status. Without this yield, if the stream died
          // unexpectedly (network error), onEndCallback and onErrorCallback
          // could both fire because _notifyEndOfStream() is called in
          // processStream's catch block before the re-throw schedules the
          // .catch handler as a microtask.
          await Promise.resolve();
          if (!internalController.signal.aborted && onEndCallback) {
            onEndCallback();
          }
        }
      } catch (err: unknown) {
        // 如果是由于取消导致的错误，使用特定的错误消息
        if (
          internalController.signal.aborted &&
          err instanceof Error &&
          err.message === "Operation aborted"
        ) {
          // onHeaders / onGoaway / processStream 错误已通过 reportError 处理，
          // 此处仅在回调尚未触发时才报告（外部取消/超时场景）
          if (!errorCallbackFired && onErrorCallback) {
            onErrorCallback(new Error("Operation cancelled by user"));
          }
        } else if (!errorCallbackFired && onErrorCallback) {
          onErrorCallback(err);
        } else if (!errorCallbackFired) {
          if (err instanceof Error) {
            console.error("asyncCall error:", err.message);
          } else {
            console.error("asyncCall error:", err);
          }
        }
      } finally {
        clearTimeout(timeoutHandle);
        // 移除外部 abort 监听器，防止 AbortController 复用时触发迟到的 cancelOperation()
        if (contextAbortHandler && context?.signal) {
          context.signal.removeEventListener("abort", contextAbortHandler);
        }
        // 首先标记操作已结束（正常或异常），确保 processStream.catch 不会把
        // writer.abort() 产生的 'Call cleanup' 错误误判为真实错误并触发 onErrorCallback。
        // internalController.abort() 是幂等的，重复调用安全。
        internalController.abort();
        // 必须先 abort writer（立即强制停止 pushable + stream），再 close stream。
        // 若顺序颠倒：stream.close() 等待服务端半关闭确认，网络异常时永久挂住，
        // writer.abort() 永远不执行 → watchdog / pushable 泄漏。
        // abort() 内部幂等，重复调用安全。
        writer?.abort('Call cleanup');
        if (stream) {
          // close() 超时会强制 abort，避免半开流长期占用 maxOutboundStreams 名额。
          await closeStreamWithTimeout(stream);
        }
        // 如果本次强制使用了新连接，结束时尽量关闭它，避免连接泄漏
        if (options?.freshConnection) {
          try {
            // 通过 libp2p 连接管理器关闭到该 peer 的连接
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const conns = (this.node as any).getConnections?.(this.peerAddr as any) || [];
            for (const c of conns) {
              try {
                await c.close?.();
              } catch { /* ignore close errors */ }
            }
          } catch { /* ignore connection cleanup errors */ }
        }
        if (streamSlotAcquired && state) {
          state.activeStreams = Math.max(0, state.activeStreams - 1);
          this.notifyStreamSlotAvailable(state);
        }
        // 本流已结束：若连接收到过 GOAWAY 且已无活跃流，此时才真正关闭。
        this.closeConnectionIfDrained(connection, state);
      }
    })();

    try {
      // 执行操作并返回取消函数
      await Promise.race([operationPromise, timeoutPromise]);
      return cancelOperation;
    } catch (error) {
      // Give the previous operation a bounded opportunity to run its finally
      // block before the caller starts another stream. This prevents timeout
      // retries from accumulating live close/end listeners.
      cancelOperation();
      let cleanupTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          operationPromise.catch(() => undefined),
          new Promise<void>((resolve) => {
            cleanupTimer = setTimeout(resolve, CALL_CLEANUP_TIMEOUT);
          }),
        ]);
      } finally {
        if (cleanupTimer !== undefined) clearTimeout(cleanupTimer);
      }
      throw error;
    }
  }
}
