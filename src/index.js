import { DurableObject } from "cloudflare:workers";

const MEMBERS = [
  {
    name: "yami",
    displayName: "야미",
    idx: "mNzL0s3DwWhrZXKAp52WpYI",
    afreecaId: "kisss2",
    platform: "afreeca",
    mvpPreset: "2",
  },
  {
    name: "seonha",
    displayName: "선하",
    idx: "mNzL0s3DwWFsZGuAp52WpYI",
    afreecaId: "ols3",
    platform: "afreeca",
    mvpPreset: "0",
  },
  {
    name: "dorit",
    displayName: "도릿",
    idx: "mNzL0s3DwWdsZG2Ap52WpYI",
    afreecaId: "chziaxz",
    platform: "afreeca",
    mvpPreset: "0",
  },
  {
    name: "anonymous",
    displayName: "익명",
    idx: "mNzL0s3DwWFrZWpWr8SWpJGbUQ",
    afreecaId: "UC-C8YE-QVl4P92I4j7XssCQ",
    platform: "youtube",
    mvpPreset: "0",
  },
];

const SERVERS = [
  "ssmain.weflab.com",
  "ssafreeca.weflab.com",
  "ssyoutube.weflab.com",
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
  anonymous: {
    displayName: "익명",
    weflab: 0,
    updatedAt: null,
  },
};

const EMPTY_MVP_ROOMS = {
  yami: {},
  seonha: {},
  dorit: {},
  anonymous: {},
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
    this.mvpRooms = structuredClone(EMPTY_MVP_ROOMS);
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

        this.gauges = {
          ...structuredClone(EMPTY_GAUGES),
          ...((await this.ctx.storage.get("gauges")) || {}),
        };

        this.mvpRooms = {
          ...structuredClone(EMPTY_MVP_ROOMS),
          ...((await this.ctx.storage.get("mvpRooms")) || this.rebuildMvpFromEvents()),
        };

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

    if (
      url.pathname === "/live" &&
      request.headers.get("Upgrade") ===
        "websocket"
    ) {
      await this.startConnections();
      const pair = new WebSocketPair();
      const [client, server] =
        Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.send(
        JSON.stringify(this.snapshot("snapshot"))
      );
      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    }

    if (url.pathname === "/mvp") {
      await this.startConnections();
      return this.json(this.snapshot("snapshot"));
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
        total: MEMBERS.reduce((sum, member) => sum + (this.gauges[member.name]?.weflab || 0), 0),
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
      total: MEMBERS.reduce((sum, member) => sum + (this.gauges[member.name]?.weflab || 0), 0),
      connections: this.connectionStatus(),
      gaugeUrl: `${url.origin}/gauges`,
      eventsUrl: `${url.origin}/events`,
      mvpUrl: `${url.origin}/mvp`,
      liveUrl: `${url.origin.replace("http", "ws")}/live`,
    });
  }

  webSocketMessage(socket, message) {
    if (message === "ping") socket.send("pong");
  }

  webSocketClose(socket) {
    try { socket.close(); } catch {}
  }

  webSocketError(socket) {
    try { socket.close(); } catch {}
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
      const channels = [
        ...SERVERS.filter((server) => server === "ssmain.weflab.com" || server === `ss${member.platform || "afreeca"}.weflab.com`).map((server) => ({
          server,
          pageid: "goal",
          preset: "0",
        })),
        {
          server: "ssmain.weflab.com",
          pageid: "subtitle",
          preset: member.mvpPreset,
        },
      ];

      for (const channel of channels) {
        const key =
          `${member.name}:${channel.server}:${channel.pageid}:${channel.preset}`;
        const current = this.sockets.get(key);

        if (
          current &&
          (
            current.readyState === WebSocket.OPEN ||
            current.readyState === WebSocket.CONNECTING
          )
        ) {
          continue;
        }

        this.connect(
          member,
          channel.server,
          key,
          channel.pageid,
          channel.preset
        );
      }
    }

    await this.ctx.storage.setAlarm(
      Date.now() + 60000
    );
  }

  connect(member, server, key, pageid = "goal", preset = "0") {
    const socketUrl =
      `wss://${server}/socket.io/` +
      `?idx=${encodeURIComponent(
        member.idx
      )}` +
      `&type=page` +
      `&page=${encodeURIComponent(pageid)}` +
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
          `ss${member.platform || "afreeca"}.weflab.com`
            ? {
                type: "join_platform",
                platform: member.platform || "afreeca",
                id: member.afreecaId,
                page: "page",
                idx: member.idx,
                pageid,
                preset,
              }
            : {
                type: "join",
                page: "page",
                idx: member.idx,
                pageid,
                preset,
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
      (payload.pageid === "goal" || payload.pageid === "subtitle")
    ) {
      this.gauges[member.name].weflab = 0;
      this.gauges[member.name].updatedAt =
        new Date().toISOString();

      await this.ctx.storage.put(
        "gauges",
        this.gauges
      );

      this.mvpRooms[member.name] = {};
      await this.ctx.storage.put(
        "mvpRooms",
        this.mvpRooms
      );
      this.broadcast(this.snapshot("snapshot"));
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

        const name = String(
          donationData.uname ||
          donationData.name ||
          ""
        ).trim();
        const rawId = String(
          donationData.uid ||
          donationData.id ||
          name
        ).trim().toLowerCase();
        let liveDonation = null;

        if (name && rawId) {
          const id =
            `${donationData.platform || "afreeca"}:${rawId}`;
          const room =
            this.mvpRooms[member.name] || {};
          const saved = room[id] || {
            id,
            name: name.slice(0, 20),
            total: 0,
          };
          saved.name = name.slice(0, 20);
          saved.total += value;
          room[id] = saved;
          this.mvpRooms[member.name] = room;
          liveDonation = {
            member: member.displayName,
            id,
            name: saved.name,
            value,
          };
        }

        await this.ctx.storage.put({
          gauges: this.gauges,
          donationKeys:
            this.donationKeys,
          mvpRooms: this.mvpRooms,
        });

        this.broadcast({
          ...this.snapshot("donation"),
          donation: liveDonation,
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

  rebuildMvpFromEvents() {
    const rooms =
      structuredClone(EMPTY_MVP_ROOMS);
    const seen = new Set();
    for (const event of [...this.events].reverse()) {
      const payload =
        Array.isArray(event.parsed)
          ? event.parsed[1]
          : null;
      const data = payload?.data;
      const value = Number(data?.value) || 0;
      const name = String(
        data?.uname || data?.name || ""
      ).trim();
      const rawId = String(
        data?.uid || data?.id || name
      ).trim().toLowerCase();
      if (!value || !name || !rawId) continue;
      const key = [
        event.member,
        data.platform || "",
        data.time || "",
        data.uid || "",
        data.value || "",
      ].join(":");
      if (seen.has(key)) continue;
      seen.add(key);
      const id =
        `${data.platform || "afreeca"}:${rawId}`;
      const room =
        rooms[event.member] ||
        (rooms[event.member] = {});
      const saved = room[id] || {
        id,
        name: name.slice(0, 20),
        total: 0,
      };
      saved.total += value;
      room[id] = saved;
    }
    return rooms;
  }

  mvpRanks() {
    const donors = new Map();
    for (const member of MEMBERS) {
      const room =
        this.mvpRooms[member.name] || {};
      for (const donor of Object.values(room)) {
        const current = donors.get(donor.id);
        if (!current || donor.total > current.total) {
          donors.set(donor.id, {
            ...donor,
            room: member.displayName,
          });
        }
      }
    }
    return [...donors.values()]
      .filter((donor) => donor.total >= 100)
      .sort((a, b) =>
        b.total - a.total ||
        a.name.localeCompare(b.name, "ko")
      )
      .slice(0, 5);
  }

  snapshot(type) {
    return {
      success: true,
      type,
      gauges: this.gauges,
      ranks: this.mvpRanks(),
      updatedAt: new Date().toISOString(),
    };
  }

  broadcast(data) {
    const message = JSON.stringify(data);
    for (const socket of this.ctx.getWebSockets()) {
      try { socket.send(message); } catch {}
    }
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
