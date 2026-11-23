import { describe, expect, it } from 'vitest';
import { ancestors, buildGraph, colOf, columnsByNode, descendants, nodeOf } from './graph';
import { model } from '../test/fixture';
import type { SiteModel } from '../types';

const g = buildGraph(model);

describe('graph', () => {
  it('builds adjacency from lineage edges only', () => {
    expect([...g.parents.get('transform:kpi.revenue')!].sort()).toEqual([
      'external:refdata.fx_rates', 'load:sales.customers', 'load:sales.orders',
    ]);
    expect(g.parents.get('load:sales.customers')!.size).toBe(0);
  });

  it('walks ancestors and descendants with a depth limit', () => {
    expect([...ancestors(g, ['transform:kpi.top_customers'], 1)]).toEqual(['transform:kpi.revenue']);
    expect(ancestors(g, ['transform:kpi.top_customers']).size).toBe(4);
    expect([...descendants(g, ['load:sales.orders'])].sort()).toEqual([
      'transform:kpi.revenue', 'transform:kpi.top_customers',
    ]);
  });

  it('terminates on cycles', () => {
    const cyclic: SiteModel = {
      ...model,
      nodes: { a: { ...model.nodes['load:sales.orders'], id: 'a' }, b: { ...model.nodes['load:sales.orders'], id: 'b' } },
      edges: [{ from: 'a', to: 'b', kind: 'lineage' }, { from: 'b', to: 'a', kind: 'lineage' }],
    };
    expect([...descendants(buildGraph(cyclic), ['a'])].sort()).toEqual(['a', 'b']);
  });

  it('tolerates a model without edges', () => {
    const bare: SiteModel = { ...model, edges: undefined, columnEdges: undefined };
    expect(buildGraph(bare).parents.get('transform:kpi.revenue')!.size).toBe(0);
    expect(columnsByNode(bare, ['load:sales.customers']).get('load:sales.customers')).toEqual(['id', 'email']);
  });

  it('splits column references', () => {
    expect(nodeOf('load:sales.customers#id')).toBe('load:sales.customers');
    expect(colOf('load:sales.customers#id')).toBe('id');
  });

  it('lists declared columns plus columns only seen in column lineage', () => {
    const cols = columnsByNode(model, ['load:sales.customers', 'transform:kpi.revenue']);
    expect(cols.get('load:sales.customers')).toEqual(['id', 'email']);
    expect(cols.get('transform:kpi.revenue')).toEqual(['customer_id']);
  });
});
