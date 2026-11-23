import { describe, expect, it } from 'vitest';
import { model } from '../test/fixture';
import { columnsByNode } from './graph';
import { columnFlowEdges, columnsAllowed, layoutInput, lineageEdges, needsSelectorPrompt, needsSizePrompt } from './flow';

describe('flow helpers', () => {
  const ids = Object.keys(model.nodes).sort();

  it('lineageEdges keeps only lineage edges inside the selection and tolerates missing edges', () => {
    const all = lineageEdges(model, new Set(ids));
    expect(all.every((e) => ids.includes(e.source) && ids.includes(e.target))).toBe(true);
    expect(lineageEdges({ ...model, edges: undefined }, new Set(ids))).toEqual([]);
    expect(lineageEdges(model, new Set())).toEqual([]);
  });

  it('layoutInput sizes nodes by column count only in column mode and defaults empty domain', () => {
    const colsByNode = new Map(ids.map((id) => [id, ['a', 'b']]));
    const plain = layoutInput(model, ids, colsByNode, false);
    const withCols = layoutInput(model, ids, colsByNode, true);
    expect(plain[0].height).toBeLessThan(withCols[0].height);
    expect(plain[0].width).toBe(withCols[0].width);
    const noDomain = layoutInput({ ...model, nodes: { x: { ...model.nodes[ids[0]], id: 'x', domain: '' } } }, ['x'], new Map(), false);
    expect(noDomain[0].group).toBe('(none)');
  });

  it('columnFlowEdges builds lowercase handles and skips edges outside the selection', () => {
    const m = {
      ...model,
      columnEdges: [
        { from: 'a#Id', to: 'b#Customer_ID', task: 't' },
        { from: 'a#x', to: 'z#y', task: 't' },
      ],
    };
    const out = columnFlowEdges(m, new Set(['a', 'b']));
    expect(out).toEqual([
      { id: 'c0', source: 'a', target: 'b', sourceHandle: 's-id', targetHandle: 't-customer_id' },
    ]);
    expect(columnFlowEdges({ ...m, columnEdges: undefined }, new Set(['a']))).toEqual([]);
  });

  it('keys a renamed column by its warehouse name (rename ?? name), case-insensitively', () => {
    const m = {
      ...model,
      nodes: {
        a: { id: 'a', kind: 'load' as const, name: 'a', domain: 'd', table: 'a' },
        b: {
          id: 'b', kind: 'load' as const, name: 'b', domain: 'd', table: 'b',
          columns: [{ name: 'cust_id', rename: 'customer_id', type: 'long', required: false, primaryKey: false }],
        },
      },
      columnEdges: [{ from: 'a#ID', to: 'b#customer_id', task: 't' }],
    };
    const cols = columnsByNode(m, ['a', 'b']);
    expect(cols.get('b')).toEqual(['customer_id']);
    const edge = columnFlowEdges(m, new Set(['a', 'b']))[0];
    const handles = new Set(cols.get('b')!.map((c) => `t-${c.toLowerCase()}`));
    expect(handles.has(edge.targetHandle!)).toBe(true);
    expect(cols.get('a')).toEqual(['ID']);
  });
});

describe('graph size limits', () => {
  it('allows the columns toggle up to 300 nodes', () => {
    expect(columnsAllowed(300)).toBe(true);
    expect(columnsAllowed(301)).toBe(false);
  });
  it('asks for a selector above 500 nodes on the whole graph', () => {
    expect(needsSelectorPrompt(500)).toBe(false);
    expect(needsSelectorPrompt(501)).toBe(true);
  });
  it('asks before rendering any selection above 500 nodes until confirmed', () => {
    expect(needsSizePrompt(500, false)).toBe(false);
    expect(needsSizePrompt(501, false)).toBe(true);
    expect(needsSizePrompt(5000, false)).toBe(true);
    expect(needsSizePrompt(501, true)).toBe(false);
    expect(needsSizePrompt(0, false)).toBe(false);
  });
});
