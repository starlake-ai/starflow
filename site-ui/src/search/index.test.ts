import { describe, expect, it } from 'vitest';
import { buildSearchIndex } from './index';
import { model } from '../test/fixture';

const index = buildSearchIndex(model);

describe('search', () => {
  it('ranks name matches first', () => {
    const hits = index.search('orders');
    expect(hits[0].nodeId).toBe('load:sales.orders');
    expect(hits[0].column).toBeUndefined();
  });

  it('returns column hits pointing at their node', () => {
    const hit = index.search('email').find((h) => h.kind === 'column');
    expect(hit).toMatchObject({ nodeId: 'load:sales.customers', column: 'email', label: 'sales.customers.email' });
  });

  it('searches descriptions, tags and SQL', () => {
    expect(index.search('master')[0].nodeId).toBe('load:sales.customers');
    expect(index.search('finance')[0].nodeId).toBe('transform:kpi.revenue');
    expect(index.search('SUM').map((h) => h.nodeId)).toContain('transform:kpi.revenue');
  });

  it('tolerates typos and prefixes', () => {
    expect(index.search('custmers').map((h) => h.nodeId)).toContain('load:sales.customers');
    expect(index.search('top_cust')[0].nodeId).toBe('transform:kpi.top_customers');
  });

  it('returns nothing for blank queries and honours the limit', () => {
    expect(index.search('  ')).toEqual([]);
    expect(index.search('sales', 1)).toHaveLength(1);
  });

  it('does not throw on duplicate column names', () => {
    const dup = structuredClone(model);
    const n = dup.nodes['load:sales.customers'];
    n.columns = [...(n.columns ?? []), ...(n.columns ?? [])];
    const idx = buildSearchIndex(dup);
    expect(idx.search('email').filter((h) => h.column === 'email')).toHaveLength(1);
  });
});
