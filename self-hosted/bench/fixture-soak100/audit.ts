import { mutationGeneric, queryGeneric } from "convex/server";
import { v } from "convex/values";
export const seed = mutationGeneric({
  args: { base: v.number(), n: v.number() },
  handler: async (ctx, { base, n }) => {
    if (base === 0)
      for (let i = 0; i < 100; i++)
        await ctx.db.insert("totals", { company: String(i), completed: 0 });
    for (let i = base; i < base + n; i++)
      await ctx.db.insert("items", {
        company: String(i % 100),
        key: i,
        body: "synthetic-only-" + i,
      });
  },
});
export const record = mutationGeneric({
  args: { seq: v.number(), company: v.string(), kind: v.string() },
  handler: async (ctx, args) => {
    const total = await ctx.db
      .query("totals")
      .withIndex("by_company", (q) => q.eq("company", args.company))
      .unique();
    if (!total) throw new Error("synthetic company missing");
    const rows = await ctx.db
      .query("items")
      .withIndex("by_company_key", (q) => q.eq("company", args.company))
      .order("desc")
      .take(100);
    for (let part = 0; part < 59; part++)
      await ctx.db.insert("writes", {
        seq: args.seq,
        company: args.company,
        part,
        body: "x".repeat(256),
      });
    await ctx.db.insert("events", {
      ...args,
      checksum: rows.reduce((sum, row) => sum + row.key, 0),
    });
    await ctx.db.patch(total._id, { completed: total.completed + 1 });
  },
});
export const counts = queryGeneric({
  args: {},
  handler: async (ctx) => {
    const rows = await ctx.db.query("events").collect();
    const totals = await ctx.db.query("totals").collect();
    const completed = totals.reduce((sum, row) => sum + row.completed, 0);
    if (completed !== rows.length)
      throw new Error("lost update in company totals");
    return {
      count: rows.length,
      unique: new Set(rows.map((row) => row.seq)).size,
      completed,
      companies: totals.length,
      perCompany: Object.fromEntries(
        totals.map((row) => [row.company, row.completed]),
      ),
    };
  },
});

export const eventsPage = queryGeneric({
  args: { cursor: v.union(v.string(), v.null()) },
  handler: async (ctx, { cursor }) => {
    const page = await ctx.db
      .query("events")
      .paginate({ cursor, numItems: 5000 });
    return {
      ...page,
      page: page.page.map((row) => ({ seq: row.seq, company: row.company })),
    };
  },
});
export const companyTotals = queryGeneric({
  args: {},
  handler: (ctx) => ctx.db.query("totals").collect(),
});
