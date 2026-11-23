import type { SiteModel, SiteStats } from './types';

export type Health = {
  warnings: { id: string; messages: string[] }[];
  externals: string[];
  mismatches: { id: string; column: string; declared?: string | null; actual?: string | null }[];
  missing: string[];
};

export function computeHealth(model: SiteModel, stats: SiteStats | null): Health {
  const nodes = Object.values(model.nodes).sort((a, b) => a.id.localeCompare(b.id));
  const tables = stats ? Object.entries(stats.tables ?? {}).sort(([a], [b]) => a.localeCompare(b)) : [];
  return {
    warnings: nodes
      .filter((n) => (n.warnings ?? []).length > 0)
      .map((n) => ({ id: n.id, messages: n.warnings ?? [] })),
    externals: nodes.filter((n) => n.kind === 'external').map((n) => n.id),
    mismatches: tables.flatMap(([id, t]) =>
      (t.columnDiff ?? [])
        .filter((d) => d.status === 'mismatch')
        .map((d) => ({ id, column: d.name, declared: d.declared, actual: d.actual })),
    ),
    missing: tables.filter(([, t]) => !t.exists).map(([id]) => id),
  };
}
