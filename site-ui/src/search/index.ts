import MiniSearch from 'minisearch';
import type { SiteModel } from '../types';

type SearchDoc = {
  id: string;
  nodeId: string;
  column?: string;
  kind: string;
  label: string;
  name: string;
  description: string;
  columns: string;
  tags: string;
  sql: string;
};

export type SearchHit = { id: string; nodeId: string; column?: string; kind: string; label: string; score: number };
export type SearchIndex = { search(q: string, limit?: number): SearchHit[] };

export function buildSearchIndex(model: SiteModel): SearchIndex {
  const ms = new MiniSearch<SearchDoc>({
    fields: ['name', 'description', 'columns', 'tags', 'sql'],
    storeFields: ['nodeId', 'column', 'kind', 'label'],
    searchOptions: { boost: { name: 5, columns: 2, tags: 2 }, prefix: true, fuzzy: 0.2 },
  });
  const docs: SearchDoc[] = [];
  for (const n of Object.values(model.nodes)) {
    const label = n.domain ? `${n.domain}.${n.table}` : n.table;
    docs.push({
      id: n.id,
      nodeId: n.id,
      kind: n.kind,
      label,
      name: `${n.name} ${n.table} ${label}`,
      description: n.description ?? '',
      columns: (n.columns ?? []).map((c) => c.name).join(' '),
      tags: (n.tags ?? []).join(' '),
      sql: n.sql?.raw ?? '',
    });
    for (const c of n.columns ?? []) {
      docs.push({
        id: `${n.id}#${c.name}`,
        nodeId: n.id,
        column: c.name,
        kind: 'column',
        label: `${label}.${c.name}`,
        name: c.name,
        description: c.description ?? '',
        columns: '',
        tags: (c.tags ?? []).join(' '),
        sql: '',
      });
    }
  }
  const seen = new Set<string>();
  ms.addAll(docs.filter((d) => !seen.has(d.id) && !!seen.add(d.id)));
  return {
    search(q: string, limit = 20): SearchHit[] {
      if (!q.trim()) return [];
      return ms
        .search(q)
        .slice(0, limit)
        .map((r) => ({
          id: String(r.id),
          nodeId: r.nodeId as string,
          column: (r.column as string | undefined) ?? undefined,
          kind: r.kind as string,
          label: r.label as string,
          score: r.score,
        }));
    },
  };
}
