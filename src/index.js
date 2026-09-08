import { DurableObject } from "cloudflare:workers";

const MEMBERS = [
  {
    name: "yami",
    displayName: "야미",
    idx: "mNzL0s3DwWhrZXKAp52WpYI",
  },
  {
    name: "seonha",
    displayName: "선하",
    idx: "mNzL0s3DwWFsZGuAp52WpYI",
  },
  {
    name: "dorit",
    displayName: "도릿",
    idx: "mNzL0s3DwWdsZG2Ap52WpYI",
  },
];

const SERVERS = [
  "ssmain.weflab.com",
  "ssafreeca.weflab.com",
];

export class GaugeCollector extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    this.ctx = ctx;
    this.env = env;
    this.sockets = new Map();
    this.events = [];
    this.startedAt = null;

    this.ready = this.ctx.blockConcurrencyWhile(async () => {
      this.events =
        (await this.ctx.storage.get("recentEvents")) || [];

      this.startedAt =
        (await this.ctx.storage.get("startedAt")) || null;
    });
  }

  async fetch(request) {
    await this.ready;

    const url = new URL(request.url);

    if (url.pathname === "/start") {
      await this.startConnections();

      return this.json({
        success: true,
        message: "위플랩 게이지 연결을 시작했습니다",
        connections: this.connectionStatus(),
      });
    }

    if (url.pathname === "/events") {
      await this.startConnections();

      return this.json({
        success: true,
        message: "최근 위플랩 수신 기록",
        events: this.events,
        connections: this.connectionStatus(),
      });
    }

    if (url.pathname === "/reset-events") {
      this.events = [];
      await this.ctx.storage.put("recentEvents", []);

      return this.json({
        success: true,
        message: "수신 기록을 초기화했습니다",
      });
    }

    await this.startConnections();

    return this.json({
      ready: true,
      collector: "GAUGE_COLLECTOR",
      message: "YAMYAM 실시간 게이지 수집기 작동 중",
      startedAt: this.startedAt,
      members: MEMBERS.map((member) => ({
        name: member.name,
        displayName: member.displayName,
      })),
      connections: this.connectionStatus(),
      recentEventCount: this.events.length,
      checkEvents: `${url.origin}/events`,
    });
  }

  async alarm() {
    await this.ready;
    await this.startConnections();
    await this.ctx.storage.setAlarm(Date.now() + 60000);
  }

  async startConnections() {
    if (!this.startedAt) {
      this.startedAt = new Date().toISOString();
      await this.ctx.storage.put("startedAt", this.startedAt);
    }

    for (const member of MEMBERS) {
      for (const server of SERVERS) {
        const key = `${member.name}:${server}`;
        const current = this.sockets.get(key);

        if (
          current &&
          (current.readyState === WebSocket.OPEN ||
            current.readyState === WebSocket.CONNECTING)
        ) {
          continue;
        }

        this.connect(member, server, key);
      }
    }

    await this.ctx.storage.setAlarm(Date.now() + 60000);
  }

  connect(member, server, key) {
    const socketUrl =
      `wss://${server}/socket.io/` +
      `?idx=${encodeURIComponent(member.idx)}` +
      `&type=page&page=goal&EIO=4&transport=websocket`;

    try {
      const socket = new WebSocket(socketUrl);
      this.sockets.set(key, socket);

      let joined = false;

      const sendJoin = () => {
        if (
          joined ||
          socket.readyState !== WebSocket.OPEN
        ) {
          return;
        }

        joined = true;

        socket.send(
          `42["msg",${JSON.stringify({
            type: "join",
            page: "page",
            idx: member.idx,
            pageid: "goal",
            preset: "0",
          })}]`
        );

        this.ctx.waitUntil(
          this.recordEvent({
            member: member.name,
            displayName: member.displayName,
            server,
            kind: "joined",
            receivedAt: new Date().toISOString(),
          })
        );
      };

      socket.addEventListener("message", (event) => {
        const text =
          typeof event.data === "string"
            ? event.data
            : "";

        if (text.startsWith("0")) {
          socket.send("40");
          setTimeout(sendJoin, 150);
          return;
        }

        if (text === "2") {
          socket.send("3");
          return;
        }

        if (text.startsWith("40")) {
          sendJoin();
          return;
        }

        if (text.startsWith("42")) {
          this.ctx.waitUntil(
            this.recordEvent({
              member: member.name,
              displayName: member.displayName,
              server,
              kind: "weflab-event",
              raw: text,
              parsed: this.parseSocketEvent(text),
              receivedAt: new Date().toISOString(),
            })
          );
        }
      });

      socket.addEventListener("close", () => {
        this.sockets.delete(key);
      });

      socket.addEventListener("error", () => {
        this.sockets.delete(key);
      });
    } catch (error) {
      this.ctx.waitUntil(
        this.recordEvent({
          member: member.name,
          displayName: member.displayName,
          server,
          kind: "connection-error",
          error: String(error),
          receivedAt: new Date().toISOString(),
        })
      );
    }
  }

  parseSocketEvent(text) {
    try {
      const packet = JSON.parse(text.slice(2));

      if (
        Array.isArray(packet) &&
        typeof packet[1] === "string"
      ) {
        try {
          packet[1] = JSON.parse(packet[1]);
        } catch {
          // 문자열 그대로 보관
        }
      }

      return packet;
    } catch {
      return null;
    }
  }

  async recordEvent(event) {
    const fingerprint = JSON.stringify({
      member: event.member,
      kind: event.kind,
      raw: event.raw || "",
    });

    const duplicated = this.events.some(
      (saved) => saved.fingerprint === fingerprint
    );

    if (duplicated && event.kind === "weflab-event") {
      return;
    }

    this.events.unshift({
      ...event,
      fingerprint,
    });

    this.events = this.events.slice(0, 50);

    await this.ctx.storage.put(
      "recentEvents",
      this.events
    );
  }

  connectionStatus() {
    return MEMBERS.map((member) => {
      const connections = SERVERS.map((server) => {
        const socket = this.sockets.get(
          `${member.name}:${server}`
        );

        return {
          server,
          connected:
            socket?.readyState === WebSocket.OPEN,
          state: socket?.readyState ?? "not-started",
        };
      });

      return {
        name: member.name,
        displayName: member.displayName,
        connections,
      };
    });
  }

  json(data, status = 200) {
    return Response.json(data, {
      status,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store",
      },
    });
  }
}

export default {
  async fetch(request, env) {
    const id =
      env.GAUGE_COLLECTOR.idFromName("yamyam-main");

    const collector =
      env.GAUGE_COLLECTOR.get(id);

    return collector.fetch(request);
  },
};
