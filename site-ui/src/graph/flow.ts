import type { SiteModel } from '../types';
import { colOf, nodeOf } from './graph';
import type { LayoutNode } from './layout';

export const NODE_W = 240;
export const HEADER_H = 36;
export const ROW_H = 20;

export type FlowEdge = { id: string; source: string; target: string; sourceHandle?: string; targetHandle?: string };

/** Lineage edges whose two ends are both selected. `edges` is absent from the model when empty. */
export function lineageEdges(model: SiteModel, ids: Set<string>): { source: string; target: string }[] {
  return (model.edges ?? [])
    .filter((e) => e.kind === 'lineage' && ids.has(e.from) && ids.has(e.to))
    .map((e) => ({ source: e.from, target: e.to }));
}

/** ELK input: node height grows with its column rows only in column mode. */
export function layoutInput(
  model: SiteModel,
  ids: string[],
  colsByNode: Map<string, string[]>,
  withColumns: boolean,
): LayoutNode[] {
  return ids.map((id) => ({
    id,
    width: NODE_W,
    height: HEADER_H + (withColumns ? (colsByNode.get(id)?.length ?? 0) * ROW_H + 8 : 0),
    group: model.nodes[id]?.domain || '(none)',
  }));
}

/** Column-level edges; handle ids are lowercase so matching is case-insensitive. */
export function columnFlowEdges(model: SiteModel, ids: Set<string>): FlowEdge[] {
  return (model.columnEdges ?? [])
    .filter((e) => ids.has(nodeOf(e.from)) && ids.has(nodeOf(e.to)))
    .map((e, i) => ({
      id: `c${i}`,
      source: nodeOf(e.from),
      target: nodeOf(e.to),
      sourceHandle: `s-${colOf(e.from).toLowerCase()}`,
      targetHandle: `t-${colOf(e.to).toLowerCase()}`,
    }));
}
