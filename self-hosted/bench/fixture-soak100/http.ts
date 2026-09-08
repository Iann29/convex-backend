import { httpRouter, httpActionGeneric, anyApi } from "convex/server";
const router = httpRouter();
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const echo = httpActionGeneric(async (_ctx, req) => {
  const bytes = await req.arrayBuffer();
  const hash = await crypto.subtle.digest("SHA-256", bytes);
  const bodySha = Array.from(new Uint8Array(hash))
    .map((x) => x.toString(16).padStart(2, "0"))
    .join("");
  const headers = new Headers({ "x-audit-response": "preserved" });
  headers.append("set-cookie", "a=1; Path=/");
  headers.append("set-cookie", "b=2; Path=/");
  return Response.json(
    {
      url: req.url,
      method: req.method,
      header: req.headers.get("x-audit-header"),
      requestId: req.headers.get("convex-request-id"),
      bodySha,
    },
    { headers },
  );
});
for (const method of [
  "GET",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "OPTIONS",
] as const)
  router.route({ pathPrefix: "/audit/echo/", method, handler: echo });
router.route({
  path: "/audit/wait",
  method: "GET",
  handler: httpActionGeneric(async (_ctx, req) => {
    const start = Date.now();
    await pause(Number(new URL(req.url).searchParams.get("delay")));
    return Response.json({ start, end: Date.now() });
  }),
});
router.route({
  path: "/audit/stream",
  method: "GET",
  handler: httpActionGeneric(
    async () =>
      new Response(
        new ReadableStream({
          async start(c) {
            c.enqueue(new TextEncoder().encode("first-"));
            await pause(50);
            c.enqueue(new TextEncoder().encode("second-"));
            await pause(50);
            c.enqueue(new TextEncoder().encode("third"));
            c.close();
          },
        }),
      ),
  ),
});
router.route({
  path: "/audit/inbound",
  method: "POST",
  handler: httpActionGeneric(async (ctx, req) => {
    const { seq, company, kind, delay } = await req.json();
    await ctx.runMutation(anyApi.audit.record, { seq, company, kind });
    await pause(delay);
    return Response.json({ seq });
  }),
});
export default router;
