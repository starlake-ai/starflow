import ELK from 'elkjs/lib/elk.bundled.js';
import type { ElkNode } from 'elkjs/lib/elk-api';

const elk = new ELK();

export type LayoutNode = { id: string; width: number; height: number; group: string };
export type LayoutGroup = { id: string; label: string; x: number; y: number; width: number; height: number };
export type LayoutResult = { positions: Map<string, { x: number; y: number }>; groups: LayoutGroup[] };

export const groupId = (group: string): string => `group:${group}`;

export async function layout(
  nodes: LayoutNode[],
  edges: { source: string; target: string }[],
  groupByDomain: boolean,
): Promise<LayoutResult> {
  const ids = new Set(nodes.map((n) => n.id));
  const elkEdges = edges
    .filter((e) => ids.has(e.source) && ids.has(e.target))
    .map((e, i) => ({ id: `e${i}`, sources: [e.source], targets: [e.target] }));
  const leaf = (n: LayoutNode): ElkNode => ({ id: n.id, width: n.width, height: n.height });

  let children: ElkNode[];
  if (groupByDomain) {
    const byGroup = new Map<string, LayoutNode[]>();
    for (const n of nodes) byGroup.set(n.group, [...(byGroup.get(n.group) ?? []), n]);
    children = [...byGroup.entries()].map(([g, ns]) => ({
      id: groupId(g),
      layoutOptions: { 'elk.padding': '[top=32,left=12,bottom=12,right=12]' },
      children: ns.map(leaf),
    }));
  } else children = nodes.map(leaf);

  const root = await elk.layout({
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'RIGHT',
      'elk.layered.spacing.nodeNodeBetweenLayers': '80',
      'elk.spacing.nodeNode': '24',
      'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
    },
    children,
    edges: elkEdges,
  });

  const positions = new Map<string, { x: number; y: number }>();
  const groups: LayoutGroup[] = [];
  for (const c of root.children ?? []) {
    if (c.id.startsWith('group:')) {
      groups.push({
        id: c.id, label: c.id.slice('group:'.length),
        x: c.x ?? 0, y: c.y ?? 0, width: c.width ?? 0, height: c.height ?? 0,
      });
      // child coordinates are relative to the group, which is what xyflow expects with parentId
      for (const g of c.children ?? []) positions.set(g.id, { x: g.x ?? 0, y: g.y ?? 0 });
    } else positions.set(c.id, { x: c.x ?? 0, y: c.y ?? 0 });
  }
  return { positions, groups };
}
