import { DurableObject } from "cloudflare:workers";

const MEMBERS = [
  {
    name: "yami",
    displayName: "야미",
    idx: "mNzL0s3DwWhrZXKAp52WpYI",
    afreecaId: "kisss2",
  },
  {
    name: "seonha",
    displayName: "선하",
    idx: "mNzL0s3DwWFsZGuAp52WpYI",
    afreecaId: "ols3",
  },
  {
    name: "dorit",
    displayName: "도릿",
    idx: "mNzL0s3DwWdsZG2Ap52WpYI",
    afreecaId: "chziaxz",
  },
];

const SERVERS = [
  "ssmain.weflab.com",
  "ssafreeca.weflab.com",
];

const EMPTY_GAUGES = {
  yami: {
    displayName: "야미",
    weflab: 0,
    updatedAt: null,
  },
  seonha: {
    displayName: "선하",
    weflab: 0,
    updatedAt: null,
  },
  dorit: {
    displayName: "도릿",
    weflab: 0,
    updatedAt: null,
  },
};

export class GaugeCollector extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    this.ctx = ctx;
    this.env = env;
    this.sockets = new Map();
    this.events = [];
    this.donationKeys = [];
    this.gauges = structuredClone(EMPTY_GAUGES);
    this.startedAt = null;

    this.ready = this.ctx.blockConcurrencyWhile(
      async () => {
        this.events =
          (await this.ctx.storage.get(
            "recentEvents"
          )) || [];

        this.donationKeys =
          (await this.ctx.storage.get(
            "donationKeys"
          )) || [];

        this.gauges =
          (await this.ctx.storage.get(
            "gauges"
          )) ||
          structuredClone(EMPTY_GAUGES);

        this.startedAt =
          (await this.ctx.storage.get(
            "startedAt"
          )) || null;
      }
    );
  }

  async fetch(request) {
    await this.ready;

    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: this.corsHeaders(),
      });
    }

    if (url.pathname === "/start") {
      await this.startConnections();

      return this.json({
        success: true,
        message:
          "위플랩 게이지 연결을 시작했습니다",
        connections: this.connectionStatus(),
      });
    }

    if (
      url.pathname === "/gauge" ||
      url.pathname === "/gauges"
    ) {
      await this.startConnections();

      return this.json({
        success: true,
        gauges: this.gauges,
        total:
          this.gauges.yami.weflab +
          this.gauges.seonha.weflab +
          this.gauges.dorit.weflab,
        connections: this.connectionStatus(),
      });
    }

    if (url.pathname === "/events") {
      await this.startConnections();

      return this.json({
        success: true,
        message: "최근 위플랩 수신 기록",
        events: this.events,
        gauges: this.gauges,
        connections: this.connectionStatus(),
      });
    }

    if (url.pathname === "/reset-events") {
      this.events = [];

      await this.ctx.storage.put(
        "recentEvents",
        []
      );

      return this.json({
        success: true,
        message: "수신 기록을 초기화했습니다",
      });
    }

    await this.startConnections();

    return this.json({
      ready: true,
      collector: "GAUGE_COLLECTOR",
      message:
        "YAMYAM 실시간 게이지 수집기 작동 중",
      startedAt: this.startedAt,
      gauges: this.gauges,
      total:
        this.gauges.yami.weflab +
        this.gauges.seonha.weflab +
        this.gauges.dorit.weflab,
      connections: this.connectionStatus(),
      gaugeUrl: `${url.origin}/gauges`,
      eventsUrl: `${url.origin}/events`,
    });
  }

  async alarm() {
    await this.ready;
    await this.startConnections();

    await this.ctx.storage.setAlarm(
      Date.now() + 60000
    );
  }

  async startConnections() {
    if (!this.startedAt) {
      this.startedAt = new Date().toISOString();

      await this.ctx.storage.put(
        "startedAt",
        this.startedAt
      );
    }

    for (const member of MEMBERS) {
      for (const server of SERVERS) {
        const key =
          `${member.name}:${server}`;

        const current =
          this.sockets.get(key);

        if (
          current &&
          (
            current.readyState ===
              WebSocket.OPEN ||
            current.readyState ===
              WebSocket.CONNECTING
          )
        ) {
          continue;
        }

        this.connect(member, server, key);
      }
    }

    await this.ctx.storage.setAlarm(
      Date.now() + 60000
    );
  }

  connect(member, server, key) {
    const socketUrl =
      `wss://${server}/socket.io/` +
      `?idx=${encodeURIComponent(
        member.idx
      )}` +
      `&type=page` +
      `&page=goal` +
      `&EIO=4` +
      `&transport=websocket`;

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

        const joinData =
          server ===
          "ssafreeca.weflab.com"
            ? {
                type: "join_platform",
                platform: "afreeca",
                id: member.afreecaId,
                page: "page",
                idx: member.idx,
                pageid: "goal",
                preset: "0",
              }
            : {
                type: "join",
                page: "page",
                idx: member.idx,
                pageid: "goal",
                preset: "0",
              };

        socket.send(
          `42["msg",${JSON.stringify(
            joinData
          )}]`
        );

        this.ctx.waitUntil(
          this.recordEvent({
            member: member.name,
            displayName:
              member.displayName,
            server,
            kind: "joined",
            joinType: joinData.type,
            receivedAt:
              new Date().toISOString(),
          })
        );
      };

      socket.addEventListener(
        "message",
        (event) => {
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
            const parsed =
              this.parseSocketEvent(text);

            if (
              Array.isArray(parsed) &&
              parsed[0] === "ping"
            ) {
              socket.send(
                `42["pong",${JSON.stringify(
                  parsed[1] || {}
                )}]`
              );
            }

            this.ctx.waitUntil(
              this.handleWeflabEvent(
                member,
                server,
                text,
                parsed
              )
            );
          }
        }
      );

      socket.addEventListener(
        "close",
        () => {
          this.sockets.delete(key);
        }
      );

      socket.addEventListener(
        "error",
        () => {
          this.sockets.delete(key);
        }
      );
    } catch (error) {
      this.ctx.waitUntil(
        this.recordEvent({
          member: member.name,
          displayName:
            member.displayName,
          server,
          kind: "connection-error",
          error: String(error),
          receivedAt:
            new Date().toISOString(),
        })
      );
    }
  }

  async handleWeflabEvent(
    member,
    server,
    raw,
    parsed
  ) {
    const payload =
      Array.isArray(parsed)
        ? parsed[1]
        : null;

    if (
      payload &&
      payload.type === "reset_page" &&
      payload.pageid === "goal"
    ) {
      this.gauges[member.name].weflab = 0;
      this.gauges[member.name].updatedAt =
        new Date().toISOString();

      await this.ctx.storage.put(
        "gauges",
        this.gauges
      );
    }

    const donationData =
      payload?.data || null;

    const isDonation =
      payload?.type === "test_donation" ||
      payload?.type === "donation" ||
      donationData?.type === "SENDBALLOON";

    if (
      isDonation &&
      donationData
    ) {
      const value =
        Number(donationData.value) || 0;

      const donationKey = [
        member.name,
        donationData.platform || "",
        donationData.time || "",
        donationData.uid || "",
        donationData.value || "",
      ].join(":");

      if (
        value > 0 &&
        !this.donationKeys.includes(
          donationKey
        )
      ) {
        this.donationKeys.unshift(
          donationKey
        );

        this.donationKeys =
          this.donationKeys.slice(0, 500);

        this.gauges[member.name].weflab +=
          value;

        this.gauges[member.name].updatedAt =
          new Date().toISOString();

        await this.ctx.storage.put({
          gauges: this.gauges,
          donationKeys:
            this.donationKeys,
        });
      }
    }

    await this.recordEvent({
      member: member.name,
      displayName:
        member.displayName,
      server,
      kind: "weflab-event",
      raw,
      parsed,
      receivedAt:
        new Date().toISOString(),
    });
  }

  parseSocketEvent(text) {
    try {
      const packet =
        JSON.parse(text.slice(2));

      if (
        Array.isArray(packet) &&
        typeof packet[1] === "string"
      ) {
        try {
          packet[1] =
            JSON.parse(packet[1]);
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
    const fingerprint =
      JSON.stringify({
        member: event.member,
        server: event.server,
        kind: event.kind,
        raw: event.raw || "",
      });

    const duplicated =
      this.events.some(
        (saved) =>
          saved.fingerprint ===
          fingerprint
      );

    if (
      duplicated &&
      event.kind === "weflab-event"
    ) {
      return;
    }

    this.events.unshift({
      ...event,
      fingerprint,
    });

    this.events =
      this.events.slice(0, 50);

    await this.ctx.storage.put(
      "recentEvents",
      this.events
    );
  }

  connectionStatus() {
    return MEMBERS.map((member) => ({
      name: member.name,
      displayName:
        member.displayName,
      connections: SERVERS.map(
        (server) => {
          const socket =
            this.sockets.get(
              `${member.name}:${server}`
            );

          return {
            server,
            connected:
              socket?.readyState ===
              WebSocket.OPEN,
            state:
              socket?.readyState ??
              "not-started",
          };
        }
      ),
    }));
  }

  corsHeaders() {
    return {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods":
        "GET, OPTIONS",
      "Access-Control-Allow-Headers":
        "Content-Type",
      "Cache-Control": "no-store",
    };
  }

  json(data, status = 200) {
    return Response.json(data, {
      status,
      headers: this.corsHeaders(),
    });
  }
}

export default {
  async fetch(request, env) {
    const id =
      env.GAUGE_COLLECTOR.idFromName(
        "yamyam-main"
      );

    const collector =
      env.GAUGE_COLLECTOR.get(id);

    return collector.fetch(request);
  },
};
