import { useEffect, useMemo, useRef, useState, type FormEvent } from 'react';
import {
  Background, Controls, Handle, Position, ReactFlow, ReactFlowProvider,
  getNodesBounds, getViewportForBounds, useReactFlow,
  type Edge, type Node, type NodeProps,
} from '@xyflow/react';
import { toPng, toSvg } from 'html-to-image';
import type { SiteModel, SiteNode } from '../types';
import { ancestors, columnsByNode, descendants, type Graph } from '../graph/graph';
import { selectWithExclude } from '../graph/selectors';
import { groupId, layout } from '../graph/layout';
import { columnFlowEdges, columnsAllowed as allowColumns, layoutInput, lineageEdges, MAX_COLUMNS, needsSizePrompt } from '../graph/flow';
import { graphHref, nodeHref } from '../router';

type SlData = { node: SiteNode; columns: string[]; showColumns: boolean; dim: boolean; focus: boolean };
type DomainData = { label: string };

function SlNode({ data }: NodeProps<Node<SlData, 'sl'>>) {
  const { node, columns, showColumns, dim, focus } = data;
  return (
    <div className={`gnode kind-${node.kind}${dim ? ' dim' : ''}${focus ? ' focus' : ''}`}>
      <Handle type="target" position={Position.Left} />
      <div className="gnode-title">
        <span className={`badge kind-${node.kind}`}>{node.kind}</span> {node.domain ? `${node.domain}.` : ''}
        {node.table}
      </div>
      <Handle type="source" position={Position.Right} />
      {showColumns && (
        <ul className="gcols">
          {columns.map((c) => (
            <li key={c}>
              <Handle id={`t-${c.toLowerCase()}`} type="target" position={Position.Left} />
              {c}
              <Handle id={`s-${c.toLowerCase()}`} type="source" position={Position.Right} />
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function DomainNode({ data }: NodeProps<Node<DomainData, 'domain'>>) {
  return (
    <div className="gdomain">
      <span>{data.label}</span>
    </div>
  );
}

const nodeTypes = { sl: SlNode, domain: DomainNode };

type Props = { model: SiteModel; graph: Graph; select: string; exclude: string; compact?: boolean; focusId?: string };

export function GraphView(props: Props) {
  return (
    <ReactFlowProvider>
      <GraphInner {...props} />
    </ReactFlowProvider>
  );
}

function GraphInner({ model, graph, select, exclude, compact = false, focusId }: Props) {
  const flow = useReactFlow();
  const wrapper = useRef<HTMLDivElement>(null);
  const total = Object.keys(model.nodes).length;
  const [selText, setSelText] = useState(select);
  const [exText, setExText] = useState(exclude);
  const [showColumns, setShowColumns] = useState(false);
  const [groupByDomain, setGroupByDomain] = useState(false);
  const [highlightId, setHighlightId] = useState<string | undefined>(focusId);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [exportError, setExportError] = useState<string | null>(null);
  const [layoutError, setLayoutError] = useState<string | null>(null);
  // The selection the user agreed to render although it is above the size limit.
  const [confirmedFor, setConfirmedFor] = useState<string | null>(null);
  // The layout request whose result is on screen; any other request is still pending.
  const [laidOut, setLaidOut] = useState<object | null>(null);

  useEffect(() => {
    setSelText(select);
    setExText(exclude);
  }, [select, exclude]);
  useEffect(() => setHighlightId(focusId), [focusId]);

  const result = useMemo(() => selectWithExclude(model, graph, select, exclude), [model, graph, select, exclude]);
  const selectionKey = `${select}\n${exclude}`;
  const needsPrompt = needsSizePrompt(result.ids.size, confirmedFor === selectionKey);
  const columnsAllowed = allowColumns(result.ids.size);
  const cols = showColumns && columnsAllowed;
  const request = useMemo(() => ({ result, cols, groupByDomain }), [result, cols, groupByDomain]);
  const laying = !needsPrompt && laidOut !== request;

  useEffect(() => {
    if (needsPrompt) return;
    let cancelled = false;
    const ids = [...result.ids].sort();
    const colsByNode = columnsByNode(model, ids);
    const group = (id: string) => model.nodes[id]?.domain || '(none)';
    const lineage = lineageEdges(model, result.ids);
    const sized = layoutInput(model, ids, colsByNode, cols);
    layout(sized, lineage, groupByDomain).then(({ positions, groups }) => {
      if (cancelled) return;
      const groupNodes: Node[] = groups.map((g) => ({
        id: g.id, type: 'domain', position: { x: g.x, y: g.y }, data: { label: g.label },
        style: { width: g.width, height: g.height }, selectable: false, draggable: false,
      }));
      const flowNodes: Node[] = ids.map((id) => ({
        id,
        type: 'sl',
        position: positions.get(id) ?? { x: 0, y: 0 },
        ...(groupByDomain ? { parentId: groupId(group(id)), extent: 'parent' as const } : {}),
        data: { node: model.nodes[id], columns: colsByNode.get(id) ?? [], showColumns: cols, dim: false, focus: false },
      }));
      const flowEdges: Edge[] = cols
        ? columnFlowEdges(model, result.ids)
        : lineage.map((e) => ({ id: `${e.source}->${e.target}`, source: e.source, target: e.target }));
      setNodes([...groupNodes, ...flowNodes]);
      setEdges(flowEdges);
      setLayoutError(null);
      setLaidOut(request);
      requestAnimationFrame(() => flow.fitView({ padding: 0.1 }));
    }).catch((err: unknown) => {
      if (cancelled) return;
      setLayoutError(`Layout failed: ${err instanceof Error ? err.message : String(err)}`);
      setLaidOut(request);
    });
    return () => {
      cancelled = true;
    };
  }, [model, request, result, cols, groupByDomain, flow, needsPrompt]);

  const lit = useMemo(
    () => (highlightId ? new Set([highlightId, ...ancestors(graph, [highlightId]), ...descendants(graph, [highlightId])]) : null),
    [graph, highlightId],
  );
  const shownNodes = useMemo(
    () => nodes.map((n) => (n.type === 'sl' ? { ...n, data: { ...n.data, dim: lit ? !lit.has(n.id) : false, focus: n.id === focusId } } : n)),
    [nodes, lit, focusId],
  );
  const shownEdges = useMemo(
    () =>
      edges.map((e) => {
        const on = !lit || (lit.has(e.source) && lit.has(e.target));
        return { ...e, animated: !!lit && on, style: on ? undefined : { opacity: 0.15 } };
      }),
    [edges, lit],
  );

  const apply = (e: FormEvent) => {
    e.preventDefault();
    window.location.hash = graphHref(selText, exText);
  };

  const exportImage = async (format: 'png' | 'svg') => {
    setExportError(null);
    try {
      await doExport(format);
    } catch (err) {
      setExportError(`Export failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  const doExport = async (format: 'png' | 'svg') => {
    const viewport = wrapper.current?.querySelector<HTMLElement>('.react-flow__viewport');
    if (!viewport) return;
    const bounds = getNodesBounds(flow.getNodes());
    const width = Math.max(bounds.width + 80, 400);
    const height = Math.max(bounds.height + 80, 300);
    const vp = getViewportForBounds(bounds, width, height, 0.05, 2, 0.05);
    const options = {
      width,
      height,
      backgroundColor: getComputedStyle(document.documentElement).getPropertyValue('--bg').trim() || '#ffffff',
      style: { width: `${width}px`, height: `${height}px`, transform: `translate(${vp.x}px, ${vp.y}px) scale(${vp.zoom})` },
    };
    const url = format === 'png' ? await toPng(viewport, options) : await toSvg(viewport, options);
    const a = document.createElement('a');
    a.href = url;
    a.download = `lineage.${format}`;
    a.click();
  };

  return (
    <div className={compact ? 'graph compact' : 'graph'} ref={wrapper}>
      {!compact && (
        <form className="graph-bar" onSubmit={apply}>
          <input value={selText} onChange={(e) => setSelText(e.target.value)} placeholder="Select, e.g. +orders+ tag:pii" />
          <input value={exText} onChange={(e) => setExText(e.target.value)} placeholder="Exclude" />
          <button type="submit">Apply</button>
          <label title={columnsAllowed ? 'Show column-level lineage' : `Disabled above ${MAX_COLUMNS} nodes`}>
            <input type="checkbox" checked={cols} disabled={!columnsAllowed} onChange={(e) => setShowColumns(e.target.checked)} />
            Columns
          </label>
          <label>
            <input type="checkbox" checked={groupByDomain} onChange={(e) => setGroupByDomain(e.target.checked)} />
            Group by domain
          </label>
          <button type="button" onClick={() => exportImage('svg')}>SVG</button>
          <button type="button" onClick={() => exportImage('png')}>PNG</button>
          <span className="muted">{result.ids.size} nodes</span>
        </form>
      )}
      {exportError && <div className="warnings" role="alert">{exportError}</div>}
      {layoutError && <div className="warnings" role="alert">{layoutError}</div>}
      {result.errors.length > 0 && (
        <ul className="warnings">
          {result.errors.map((e, i) => (
            <li key={`${i}-${e}`}>{e}</li>
          ))}
        </ul>
      )}
      {needsPrompt ? (
        <div className="empty">
          {!compact && !select.trim() && (
            <p>
              This project has {total} nodes. Enter a selector, for example <code>+orders+</code> or <code>domain:sales</code>.
            </p>
          )}
          <button type="button" onClick={() => setConfirmedFor(selectionKey)}>
            {`${result.ids.size} nodes selected, render anyway`}
          </button>
        </div>
      ) : (
        <ReactFlow
          nodes={shownNodes}
          edges={shownEdges}
          nodeTypes={nodeTypes}
          nodesDraggable={false}
          nodesConnectable={false}
          onlyRenderVisibleElements
          minZoom={0.05}
          onNodeClick={(_, n) => n.type === 'sl' && setHighlightId((cur) => (cur === n.id ? undefined : n.id))}
          onNodeDoubleClick={(_, n) => {
            if (n.type === 'sl') window.location.hash = nodeHref(n.id);
          }}
          onPaneClick={() => setHighlightId(focusId)}
        >
          <Background />
          <Controls showInteractive={false} />
          {laying && (
            <div className="graph-status" role="status">
              Laying out...
            </div>
          )}
        </ReactFlow>
      )}
    </div>
  );
}
