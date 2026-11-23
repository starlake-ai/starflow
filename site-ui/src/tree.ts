import type { SiteModel } from './types';

export type TreeNode = { key: string; label: string; nodeId?: string; children: TreeNode[] };

const leaf = (model: SiteModel, parentKey: string, id: string): TreeNode => ({
  key: `${parentKey}/${id}`,
  label: model.nodes[id].name,
  nodeId: id,
  children: [],
});

const branch = (model: SiteModel, key: string, label: string, ids: string[]): TreeNode => ({
  key,
  label,
  children: ids.filter((id) => id in model.nodes).map((id) => leaf(model, key, id)),
});

export function projectTree(model: SiteModel): TreeNode[] {
  const g = model.groups;
  return [
    {
      key: 'load',
      label: 'Load',
      children: (g.domains ?? []).map((d) => branch(model, `load/${d.name}`, d.name, d.nodes)),
    },
    {
      key: 'transform',
      label: 'Transform',
      children: (g.jobs ?? []).map((j) => branch(model, `transform/${j.name}`, j.name, j.nodes)),
    },
    {
      key: 'dags',
      label: 'DAGs',
      children: (g.dags ?? []).map((d) => branch(model, `dags/${d.name}`, d.name, d.nodes)),
    },
  ];
}

export function databaseTree(model: SiteModel): TreeNode[] {
  const byDb = new Map<string, Map<string, string[]>>();
  for (const n of Object.values(model.nodes)) {
    const db = n.database || n.connectionRef || 'default';
    const schema = n.domain || '(none)';
    if (!byDb.has(db)) byDb.set(db, new Map());
    const schemas = byDb.get(db)!;
    if (!schemas.has(schema)) schemas.set(schema, []);
    schemas.get(schema)!.push(n.id);
  }
  const sorted = <T,>(m: Map<string, T>) => [...m.entries()].sort(([a], [b]) => a.localeCompare(b));
  return sorted(byDb).map(([db, schemas]) => ({
    key: `db/${db}`,
    label: db,
    children: sorted(schemas).map(([schema, ids]) =>
      branch(
        model,
        `db/${db}/${schema}`,
        schema,
        [...ids].sort((a, b) => model.nodes[a].table.localeCompare(model.nodes[b].table)),
      ),
    ),
  }));
}

export function filterTree(nodes: TreeNode[], q: string): TreeNode[] {
  const query = q.trim().toLowerCase();
  if (!query) return nodes;
  const keep = (n: TreeNode): TreeNode | null => {
    if (n.nodeId) {
      return n.label.toLowerCase().includes(query) || n.nodeId.includes(query) ? n : null;
    }
    const children = n.children.map(keep).filter((c): c is TreeNode => c !== null);
    return children.length > 0 ? { ...n, children } : null;
  };
  return nodes.map(keep).filter((n): n is TreeNode => n !== null);
}

/** Keys of every branch on a path to the leaf of `nodeId` (a node can sit under several branches). */
export function pathTo(nodes: TreeNode[], nodeId: string): string[] {
  const keys: string[] = [];
  const visit = (n: TreeNode): boolean => {
    if (n.nodeId) return n.nodeId === nodeId;
    let found = false;
    for (const c of n.children) if (visit(c)) found = true;
    if (found) keys.push(n.key);
    return found;
  };
  nodes.forEach(visit);
  return keys;
}
