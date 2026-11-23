import { describe, expect, it } from 'vitest';
import { renderToString } from 'react-dom/server';
import { GraphView } from './GraphView';
import { buildGraph } from '../graph/graph';
import type { SiteModel, SiteNode } from '../types';
import { renderPage } from '../test/render';

// A chain of n load nodes in one domain, linked by lineage edges.
function big(n: number): SiteModel {
  const nodes: Record<string, SiteNode> = {};
  for (let i = 0; i < n; i++) {
    const id = `load:d.t${i}`;
    nodes[id] = { id, kind: 'load', name: `t${i}`, domain: 'd', table: `t${i}` };
  }
  const edges = Array.from({ length: n - 1 }, (_, i) => ({ from: `load:d.t${i}`, to: `load:d.t${i + 1}`, kind: 'lineage' as const }));
  return { schemaVersion: 1, generatedAt: 'x', starlakeVersion: 'x', project: { name: 'p' }, nodes, edges, groups: {} };
}

const render = (m: SiteModel, select: string, compact = false) =>
  renderToString(<GraphView model={m} graph={buildGraph(m)} select={select} exclude="" compact={compact} />).replace(
    /<!-- -->/g,
    '',
  );

describe('GraphView size guard', () => {
  it('prompts for a selector on a large project with no selector, with a render anyway button', () => {
    const html = render(big(501), '');
    expect(html).toContain('This project has 501 nodes');
    expect(html).toContain('501 nodes selected, render anyway');
  });

  it('prompts on a non empty selector whose selection exceeds the limit', () => {
    const html = render(big(600), 't0+');
    expect(html).toContain('600 nodes selected, render anyway');
    expect(html).not.toContain('react-flow__renderer');
  });

  it('renders a selection within the limit without a prompt', () => {
    const html = render(big(600), 't0+3');
    expect(html).not.toContain('render anyway');
    expect(html).toContain('4 nodes');
  });

  it('shows the layout state while ELK runs', () => {
    expect(render(big(3), '')).toContain('Laying out...');
  });
});

describe('GraphView selector errors', () => {
  it('lists repeated errors without a duplicate key error', () => {
    const m = big(3);
    const { container, errors } = renderPage(<GraphView model={m} graph={buildGraph(m)} select="nope nope" exclude="" />);
    expect(container.querySelectorAll('ul.warnings li')).toHaveLength(2);
    expect(errors().filter((e) => e.includes('same key'))).toEqual([]);
  });
});
