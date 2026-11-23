import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { renderToString } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { normalizeModel } from './data/load';
import { buildGraph } from './graph/graph';
import { select } from './graph/selectors';
import { buildSearchIndex } from './search/index';
import { databaseTree, projectTree } from './tree';
import { computeHealth } from './health';
import { Overview } from './components/Overview';
import { NodePage } from './components/NodePage';
import type { SiteModel } from './types';

// The golden file the Scala exporter test compares against: the UI must accept exactly that shape.
const here = dirname(fileURLToPath(import.meta.url));
const golden = resolve(here, '../../src/test/resources/site/expected-sl-model.json');
const model = normalizeModel(JSON.parse(readFileSync(golden, 'utf8')) as SiteModel);
const graph = buildGraph(model);
const text = (html: string) => html.replace(/<!-- -->/g, '');

describe('contract with the exporter golden file', () => {
  const nodes = Object.values(model.nodes);

  it('has nodes to check', () => {
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('builds the trees and the search index', () => {
    const leaves = (ns: ReturnType<typeof projectTree>): string[] =>
      ns.flatMap((n) => (n.nodeId ? [n.nodeId] : leaves(n.children)));
    expect(new Set(leaves(databaseTree(model)))).toEqual(new Set(Object.keys(model.nodes)));
    expect(leaves(projectTree(model)).length).toBeGreaterThan(0);
    const index = buildSearchIndex(model);
    for (const n of nodes) expect(index.search(n.name).map((h) => h.nodeId)).toContain(n.id);
  });

  it('selects the ancestors of a transform', () => {
    const target = nodes.find((n) => n.kind === 'transform' && (graph.parents.get(n.id)?.size ?? 0) > 0)!;
    const { ids, errors } = select(model, graph, `+${target.name}`);
    expect(errors).toEqual([]);
    expect(ids.has(target.id)).toBe(true);
    expect(ids.size).toBeGreaterThan(1);
  });

  it('covers the declared external node and groups.externals', () => {
    const ext = model.nodes['external:refdata.fx_rates'];
    expect(ext.kind).toBe('external');
    expect(ext.undeclared).toBeUndefined();
    expect((ext.columns ?? []).length).toBeGreaterThan(0);
    expect(ext.filePath).toBe('external/refdata/fx_rates.sl.yml');
    expect(model.groups.externals).toEqual([
      { name: 'refdata', description: expect.any(String), nodes: ['external:refdata.fx_rates'] },
    ]);
    const tree = projectTree(model).find((n) => n.key === 'external')!;
    expect(tree.children[0].children.map((n) => n.nodeId)).toEqual(['external:refdata.fx_rates']);
    expect(computeHealth(model, null).externals).toEqual([]);
    const html = text(renderToString(<Overview model={model} stats={null} />));
    expect(html).toContain('External domains');
  });

  it('renders the overview', () => {
    const html = text(renderToString(<Overview model={model} stats={null} />));
    for (const d of model.groups.domains ?? []) expect(html).toContain(d.name);
  });

  it.each(Object.keys(model.nodes))('renders the page of %s', (id) => {
    const node = model.nodes[id];
    const html = text(renderToString(<NodePage model={model} stats={null} graph={graph} node={node} />));
    expect(html).toContain(node.name);
  });
});
