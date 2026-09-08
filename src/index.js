import { DurableObject } from "cloudflare:workers";

export class GaugeCollector extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
  }

  async fetch(request) {
    return Response.json({
      ready: true,
      collector: "GAUGE_COLLECTOR",
      message: "YAMYAM 실시간 게이지 수집기 연결 완료",
    });
  }
}

export default {
  async fetch(request, env) {
    const id = env.GAUGE_COLLECTOR.idFromName("yamyam-main");
    const collector = env.GAUGE_COLLECTOR.get(id);
    return collector.fetch(request);
  },
};
