import assert from "node:assert/strict";

// Verification runs after emission and drain; each query stays below the read limit.
export async function countFixture(client) {
  const ids = new Set(),
    observed = new Map();
  let cursor = null;
  for (;;) {
    const result = await client.query("audit:eventsPage", { cursor });
    for (const row of result.page) {
      assert(!ids.has(row.seq), "duplicate event sequence");
      ids.add(row.seq);
      observed.set(row.company, (observed.get(row.company) ?? 0) + 1);
    }
    if (result.isDone) break;
    assert(result.continueCursor !== cursor, "pagination did not advance");
    cursor = result.continueCursor;
  }
  const totals = await client.query("audit:companyTotals", {});
  assert.equal(totals.length, 100);
  for (const total of totals)
    assert.equal(total.completed, observed.get(total.company) ?? 0);
  const completed = totals.reduce((sum, row) => sum + row.completed, 0);
  assert.equal(completed, ids.size);
  return {
    count: ids.size,
    unique: ids.size,
    completed,
    companies: totals.length,
    perCompany: Object.fromEntries(
      totals.map((row) => [row.company, row.completed]),
    ),
  };
}
