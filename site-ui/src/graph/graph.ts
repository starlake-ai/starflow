import type { SiteModel } from '../types';

export type Graph = { parents: Map<string, Set<string>>; children: Map<string, Set<string>> };

export function buildGraph(model: SiteModel): Graph {
  const parents = new Map<string, Set<string>>();
  const children = new Map<string, Set<string>>();
  for (const id of Object.keys(model.nodes)) {
    parents.set(id, new Set());
    children.set(id, new Set());
  }
  for (const e of model.edges ?? []) {
    if (e.kind !== 'lineage' || !parents.has(e.to) || !parents.has(e.from)) continue;
    parents.get(e.to)!.add(e.from);
    children.get(e.from)!.add(e.to);
  }
  return { parents, children };
}

function walk(adj: Map<string, Set<string>>, start: Iterable<string>, depth: number): Set<string> {
  const seen = new Set<string>();
  let frontier = [...start];
  let level = 0;
  while (frontier.length > 0 && level < depth) {
    const next: string[] = [];
    for (const id of frontier) {
      for (const n of adj.get(id) ?? []) {
        if (!seen.has(n)) {
          seen.add(n);
          next.push(n);
        }
      }
    }
    frontier = next;
    level++;
  }
  return seen;
}

export const ancestors = (g: Graph, ids: Iterable<string>, depth = Infinity): Set<string> =>
  walk(g.parents, ids, depth);

export const descendants = (g: Graph, ids: Iterable<string>, depth = Infinity): Set<string> =>
  walk(g.children, ids, depth);

export const nodeOf = (ref: string): string => ref.slice(0, ref.lastIndexOf('#'));
export const colOf = (ref: string): string => ref.slice(ref.lastIndexOf('#') + 1);

export function columnsByNode(model: SiteModel, ids: string[]): Map<string, string[]> {
  const result = new Map<string, string[]>(
    ids.map((id) => [id, (model.nodes[id]?.columns ?? []).map((c) => c.rename ?? c.name)]),
  );
  for (const e of model.columnEdges ?? []) {
    for (const ref of [e.from, e.to]) {
      const list = result.get(nodeOf(ref));
      const col = colOf(ref);
      if (list && !list.some((c) => c.toLowerCase() === col.toLowerCase())) list.push(col);
    }
  }
  return result;
}
