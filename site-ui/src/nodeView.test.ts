import { describe, expect, it } from 'vitest';
import { model } from './test/fixture';
import { buildGraph } from './graph/graph';
import { codeTabs, diffIndex, extraColumns, relations } from './nodeView';
import type { SiteNode } from './types';

describe('diffIndex', () => {
  it('indexes by lowercase column name and tolerates a missing diff', () => {
    const idx = diffIndex([{ name: 'Email', status: 'mismatch', declared: 'A', actual: 'B' }]);
    expect(idx.get('email')?.status).toBe('mismatch');
    expect(diffIndex(undefined).size).toBe(0);
  });
});

describe('extraColumns', () => {
  it('keeps only extraInWarehouse entries', () => {
    const d = [
      { name: 'a', status: 'match' as const },
      { name: 'b', status: 'extraInWarehouse' as const, actual: 'INT' },
    ];
    expect(extraColumns(d).map((x) => x.name)).toEqual(['b']);
    expect(extraColumns(undefined)).toEqual([]);
  });
});

describe('relations', () => {
  const g = buildGraph(model);
  it('lists sorted lineage parents and children, fk both ways', () => {
    const r = relations(model, g, 'transform:kpi.revenue');
    expect(r.dependsOn).toEqual(['external:refdata.fx_rates', 'load:sales.customers', 'load:sales.orders']);
    expect(r.referencedBy).toEqual(['transform:kpi.top_customers']);
    expect(relations(model, g, 'load:sales.orders').fkTo).toEqual(['load:sales.customers']);
    expect(relations(model, g, 'load:sales.customers').fkFrom).toEqual(['load:sales.orders']);
  });
  it('handles a model with no edges', () => {
    const m = { ...model, edges: undefined };
    const r = relations(m, buildGraph(m), 'load:sales.orders');
    expect(r.fkTo).toEqual([]);
  });
});

describe('codeTabs', () => {
  const base = { id: 'x', kind: 'transform', name: 'x', domain: 'd', table: 'x' } as SiteNode;
  it('builds tabs in order and skips empty ones', () => {
    const tabs = codeTabs({ ...base, sql: { raw: 'select 1', compiled: 'select 2' }, presql: ['a', 'b'], postsql: [] });
    expect(tabs.map((t) => t.key)).toEqual(['raw', 'compiled', 'pre']);
    expect(tabs[2].code).toBe('a;\nb');
  });
  it('returns none when no code', () => {
    expect(codeTabs(base)).toEqual([]);
  });
});
