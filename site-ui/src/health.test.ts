import { describe, expect, it } from 'vitest';
import { computeHealth } from './health';
import { model, stats } from './test/fixture';

describe('computeHealth', () => {
  it('lists warnings and external references without stats', () => {
    const h = computeHealth(model, null);
    expect(h.warnings).toEqual([{ id: 'load:sales.orders', messages: ["Unknown doc block 'missing_block'"] }]);
    expect(h.externals).toEqual(['external:refdata.fx_rates']);
    expect(h.mismatches).toEqual([]);
    expect(h.missing).toEqual([]);
  });

  it('adds type mismatches and missing tables with stats', () => {
    const h = computeHealth(model, stats);
    expect(h.mismatches).toEqual([
      { id: 'load:sales.customers', column: 'email', declared: 'VARCHAR', actual: 'TEXT' },
    ]);
    expect(h.missing).toEqual(['load:sales.orders']);
  });

  it('tolerates stats without tables', () => {
    const h = computeHealth(model, { schemaVersion: 1, generatedAt: 'x' });
    expect(h.mismatches).toEqual([]);
    expect(h.missing).toEqual([]);
  });
});
