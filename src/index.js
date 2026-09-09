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
    this.collectorEnabled = true;
    this.enabledAt = 0;
    this.lastGaugeReconcileAt = 0;

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

        // 2026-09-09: 야미 개인 후원순위는 이미 초기화됐지만 당시
        // subtitle 채널을 직접 구독하지 않아 신호를 놓쳤다. 딱 한 번만
        // 남아 있는 야미 방 MVP를 정리하고 이후에는 실시간 reset_page로 처리한다.
        const yamiResetMigration = "mvpResetMigration:yami:2026-09-09";
        if (!(await this.ctx.storage.get(yamiResetMigration))) {
          this.mvpRooms.yami = {};
          await this.ctx.storage.put({
            mvpRooms: this.mvpRooms,
            [yamiResetMigration]: true,
          });
        }

        if (this.applyRecentSubtitleResets()) {
          await this.ctx.storage.put("mvpRooms", this.mvpRooms);
        }

        this.startedAt =
          (await this.ctx.storage.get(
            "startedAt"
          )) || null;

        const storedEnabled = await this.ctx.storage.get("collectorEnabled");
        this.collectorEnabled = storedEnabled !== false;
        this.enabledAt = Number(await this.ctx.storage.get("enabledAt")) || 0;
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

    if (url.pathname === "/control" && request.method === "POST") {
      let body = {};
      try {
        body = await request.json();
      } catch {
        return this.json({ success: false, error: "잘못된 요청입니다" }, 400);
      }
      if (typeof body.enabled !== "boolean")
        return this.json({ success: false, error: "enabled 값이 필요합니다" }, 400);

      this.collectorEnabled = body.enabled;
      if (body.enabled) {
        this.enabledAt = Date.now();
        await this.ctx.storage.put({
          collectorEnabled: true,
          enabledAt: this.enabledAt,
        });
        await this.startConnections();
      } else {
        await this.ctx.storage.put("collectorEnabled", false);
        await this.closeUpstreamConnections();
        await this.ctx.storage.deleteAlarm();
      }
      return this.json(this.snapshot("control"));
    }

    if (
      url.pathname === "/live" &&
      request.headers.get("Upgrade") ===
        "websocket"
    ) {
      if (this.collectorEnabled) await this.startConnections();
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
      if (this.collectorEnabled) await this.startConnections();
      return this.json(this.snapshot("snapshot"));
    }

    if (url.pathname === "/start") {
      if (this.collectorEnabled) await this.startConnections();

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
      if (this.collectorEnabled) await this.startConnections();

      return this.json({
        success: true,
        gauges: this.gauges,
        total: MEMBERS.reduce((sum, member) => sum + (this.gauges[member.name]?.weflab || 0), 0),
        connections: this.connectionStatus(),
      });
    }

    if (url.pathname === "/events") {
      if (this.collectorEnabled) await this.startConnections();

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

    if (this.collectorEnabled) await this.startConnections();

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
    if (!this.collectorEnabled) return;
    await this.startConnections();

    await this.ctx.storage.setAlarm(
      Date.now() + 60000
    );
  }

  async startConnections() {
    if (!this.collectorEnabled) return;
    if (!this.startedAt) {
      this.startedAt = new Date().toISOString();

      await this.ctx.storage.put(
        "startedAt",
        this.startedAt
      );
    }

    for (const member of MEMBERS) {
      for (const channel of [
        { pageid: "goal", preset: "0" },
        { pageid: "subtitle", preset: member.mvpPreset || "0" },
      ]) {
      for (const server of SERVERS) {
        if (server !== "ssmain.weflab.com" && server !== `ss${member.platform || "afreeca"}.weflab.com`) continue;
        const key =
          `${member.name}:${channel.pageid}:${server}`;

        const current =
          this.sockets.get(key);

        if (
          current &&
          (
            current.readyState === WebSocket.OPEN ||
            current.readyState === WebSocket.CONNECTING
          )
        ) {
          continue;
        }

        this.connect(member, server, key, channel);
      }
      }
    }

    if (Date.now() - this.lastGaugeReconcileAt >= 30000) {
      this.lastGaugeReconcileAt = Date.now();
      this.ctx.waitUntil(this.reconcileGauges());
    }

    await this.ctx.storage.setAlarm(
      Date.now() + 60000
    );
  }

  async reconcileGauges() {
    if (!this.collectorEnabled) return;
    let changed = false;
    for (const member of MEMBERS) {
      try {
        const body = new URLSearchParams({
          type: "goal_load",
          pagetype: "page",
          idx: member.idx,
          pageid: "goal",
          preset: "0",
          start: "",
          reset: "",
          autoreset: "autoreset",
          date: "",
          resettype: "load",
        });
        body.set("ver[server]", "20240607");
        body.set("ver[socket]", "20240607");
        const response = await fetch("https://weflab.com/api/", {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
          },
          body,
        });
        if (!response.ok) continue;
        const json = await response.json();
        const rows = Array.isArray(json.data) ? json.data : [];
        const total = rows.reduce((sum, row) => {
          const rawTime = row?.time;
          const eventTime = typeof rawTime === "number"
            ? rawTime
            : Date.parse(String(rawTime || "").replace(" ", "T") + "+09:00");
          if (this.enabledAt && (!Number.isFinite(eventTime) || eventTime < this.enabledAt)) return sum;
          const value = Number(row?.real ?? row?.value ?? 0);
          return sum + (Number.isFinite(value) && value > 0 ? value : 0);
        }, 0);
        if ((this.gauges[member.name]?.weflab || 0) !== total) {
          this.gauges[member.name].weflab = total;
          this.gauges[member.name].updatedAt = new Date().toISOString();
          changed = true;
        }
      } catch (error) {
        await this.recordEvent({
          member: member.name,
          displayName: member.displayName,
          server: "weflab-api",
          kind: "reconcile-error",
          error: String(error),
          receivedAt: new Date().toISOString(),
        });
      }
    }
    if (changed) {
      await this.ctx.storage.put("gauges", this.gauges);
      this.broadcast(this.snapshot("snapshot"));
    }
  }

  async closeUpstreamConnections() {
    for (const socket of this.sockets.values()) {
      try { socket.close(1000, "collector-off"); } catch {}
    }
    this.sockets.clear();
  }

  connect(member, server, key, channel) {
    const socketUrl =
      `wss://${server}/socket.io/` +
      `?idx=${encodeURIComponent(
        member.idx
      )}` +
      `&type=page` +
      `&page=${encodeURIComponent(channel.pageid)}` +
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
                pageid: channel.pageid,
                preset: channel.preset,
              }
            : {
                type: "join",
                page: "page",
                idx: member.idx,
                pageid: channel.pageid,
                preset: channel.preset,
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
          if (this.sockets.get(key) === socket) this.sockets.delete(key);
        }
      );

      socket.addEventListener(
        "error",
        () => {
          if (this.sockets.get(key) === socket) this.sockets.delete(key);
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

    // OFF로 바뀌는 순간 이미 도착 중이던 메시지도 저장하지 않는다.
    if (!this.collectorEnabled) return;

    if (payload?.type === "reset_page") {
      if (payload.pageid === "goal") {
        this.gauges[member.name].weflab = 0;
        this.gauges[member.name].updatedAt = new Date().toISOString();
        await this.ctx.storage.put("gauges", this.gauges);
        this.broadcast(this.snapshot("snapshot"));
      }

      if (payload.pageid === "subtitle") {
        this.mvpRooms[member.name] = {};
        await this.ctx.storage.put("mvpRooms", this.mvpRooms);
        this.broadcast(this.snapshot("snapshot"));
      }
    }

    const donationData =
      payload?.data || null;

    const isDonation =
      payload?.type === "test_donation" ||
      payload?.type === "donation" ||
      donationData?.type === "SENDBALLOON" ||
      donationData?.subtype === "SENDBALLOON";

    if (
      isDonation &&
      donationData
    ) {
      const eventTime = Number(donationData.time) || 0;
      // 재연결 시 위플랩이 과거 메시지를 다시 보내도 ON 이전 후원은 폐기한다.
      if (eventTime && this.enabledAt && eventTime < this.enabledAt) {
        await this.recordEvent({
          member: member.name,
          displayName: member.displayName,
          server,
          kind: "ignored-before-enabled",
          receivedAt: new Date().toISOString(),
        });
        return;
      }
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
      connections: ["goal", "subtitle"].flatMap((pageid) => SERVERS.filter((server) => server === "ssmain.weflab.com" || server === `ss${member.platform || "afreeca"}.weflab.com`).map(
        (server) => {
          const socket =
            this.sockets.get(
              `${member.name}:${pageid}:${server}`
            );

          return {
            server,
            pageid,
            connected:
              socket?.readyState ===
              WebSocket.OPEN,
            state:
              socket?.readyState ??
              "not-started",
          };
        }
      )),
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

  applyRecentSubtitleResets() {
    const resetMembers = new Set();
    const seen = new Set();
    let changed = false;

    for (const event of [...this.events].reverse()) {
      const payload = Array.isArray(event.parsed) ? event.parsed[1] : null;
      if (payload?.type === "reset_page" && payload.pageid === "subtitle") {
        this.mvpRooms[event.member] = {};
        resetMembers.add(event.member);
        changed = true;
        continue;
      }
      if (!resetMembers.has(event.member)) continue;

      const data = payload?.data;
      const value = Number(data?.value) || 0;
      const name = String(data?.uname || data?.name || "").trim();
      const rawId = String(data?.uid || data?.id || name).trim().toLowerCase();
      if (!value || !name || !rawId) continue;
      const donationKey = [
        event.member,
        data.platform || "",
        data.time || "",
        data.uid || "",
        data.value || "",
      ].join(":");
      if (seen.has(donationKey)) continue;
      seen.add(donationKey);

      const id = `${data.platform || "afreeca"}:${rawId}`;
      const room = this.mvpRooms[event.member] || {};
      const saved = room[id] || { id, name: name.slice(0, 20), total: 0 };
      saved.name = name.slice(0, 20);
      saved.total += value;
      room[id] = saved;
      this.mvpRooms[event.member] = room;
    }
    return changed;
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
      .filter((donor) => donor.total >= 1)
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
      collectorEnabled: this.collectorEnabled,
      enabledAt: this.enabledAt || null,
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
        "GET, POST, OPTIONS",
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
