import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
export default defineSchema({
  items: defineTable({
    company: v.string(),
    key: v.number(),
    body: v.string(),
  }).index("by_company_key", ["company", "key"]),
  events: defineTable({
    seq: v.number(),
    company: v.string(),
    kind: v.string(),
    checksum: v.number(),
  }).index("by_company_seq", ["company", "seq"]),
  writes: defineTable({
    seq: v.number(),
    company: v.string(),
    part: v.number(),
    body: v.string(),
  }).index("by_company_seq_part", ["company", "seq", "part"]),
  totals: defineTable({ company: v.string(), completed: v.number() }).index(
    "by_company",
    ["company"],
  ),
});
