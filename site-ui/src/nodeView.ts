import type { Graph } from './graph/graph';
import type { ColumnDiff, SiteModel, SiteNode } from './types';

export const diffIndex = (diff?: ColumnDiff[]): Map<string, ColumnDiff> =>
  new Map((diff ?? []).map((d) => [d.name.toLowerCase(), d]));

export const extraColumns = (diff?: ColumnDiff[]): ColumnDiff[] =>
  (diff ?? []).filter((d) => d.status === 'extraInWarehouse');

export type Relations = { dependsOn: string[]; referencedBy: string[]; fkTo: string[]; fkFrom: string[] };

export function relations(model: SiteModel, graph: Graph, id: string): Relations {
  const edges = model.edges ?? [];
  const fk = edges.filter((e) => e.kind === 'fk');
  return {
    dependsOn: [...(graph.parents.get(id) ?? [])].sort(),
    referencedBy: [...(graph.children.get(id) ?? [])].sort(),
    fkTo: fk.filter((e) => e.from === id).map((e) => e.to).sort(),
    fkFrom: fk.filter((e) => e.to === id).map((e) => e.from).sort(),
  };
}

export type CodeTab = { key: string; label: string; code: string };

export function codeTabs(node: SiteNode): CodeTab[] {
  const tabs: CodeTab[] = [];
  if (node.sql?.raw) tabs.push({ key: 'raw', label: 'Raw SQL', code: node.sql.raw });
  if (node.sql?.compiled) tabs.push({ key: 'compiled', label: 'Compiled SQL', code: node.sql.compiled });
  if (node.presql?.length) tabs.push({ key: 'pre', label: 'Pre SQL', code: node.presql.join(';\n') });
  if (node.postsql?.length) tabs.push({ key: 'post', label: 'Post SQL', code: node.postsql.join(';\n') });
  return tabs;
}
