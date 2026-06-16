import React, { createElement as h, useState, useMemo, useEffect, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import {
  ReactFlow, ReactFlowProvider, Position, MarkerType, Handle,
  useNodesState, useEdgesState, useReactFlow, Background, Controls
} from '@xyflow/react';
import dagre from 'dagre';

/* ---- Convert ColLineage JSON (tasks) to diagram format ---- */
function convertLineageFormat(input) {
  if (!input || !input.tables) return { items: [], relations: [] };
  const items = input.tables.map(function (table) {
    const dp = table.domain ? table.domain.toLowerCase() + '.' : '';
    return {
      id: (dp + table.table).toLowerCase(),
      label: table.table,
      domain: table.domain || '',
      displayType: 'table',
      isTask: table.isTask || false,
      columns: (table.columns || []).map(function (col) {
        return { id: (dp + table.table).toLowerCase() + '.' + col.toLowerCase(), name: col, primaryKey: false, foreignKey: false };
      })
    };
  });
  const relations = (input.relations || []).map(function (r) {
    const sp = r.from.domain ? r.from.domain.toLowerCase() + '.' : '';
    const tp = r.to.domain ? r.to.domain.toLowerCase() + '.' : '';
    return {
      source: sp + r.from.table.toLowerCase() + '.' + (r.from.column || '').toLowerCase(),
      target: tp + r.to.table.toLowerCase() + '.' + (r.to.column || '').toLowerCase(),
      expression: r.expression || ''
    };
  });
  return { items, relations };
}

/* ---- Table dependencies data is already in diagram format ---- */
function convertTableDepsFormat(input) {
  if (!input || !input.items) return { items: [], relations: [] };
  const items = input.items.map(function (item) {
    return {
      ...item,
      id: item.id.toLowerCase(),
      columns: (item.columns || []).map(function (c) { return { ...c, id: c.id.toLowerCase() }; })
    };
  });
  const relations = (input.relations || []).map(function (r) {
    return { source: r.source.toLowerCase(), target: r.target.toLowerCase(), expression: r.relationType || '' };
  });
  return { items, relations };
}

/* ---- Enrich columns with source/target flags, resolve table-level connections ---- */
function enrichSchema(schema) {
  const models = (schema.items || []).map(function (item) {
    const cols = (item.columns || []).map(function (col) {
      return {
        ...col,
        isSource: (schema.relations || []).some(function (r) { return r.source === col.id; }),
        isTarget: (schema.relations || []).some(function (r) { return r.target === col.id; })
      };
    });
    return { ...item, columns: cols };
  });
  const connections = (schema.relations || []).map(function (rel) {
    /* Try column-level match first, then fall back to table-level match */
    const srcByCol = models.find(function (m) { return (m.columns || []).some(function (c) { return c.id === rel.source; }); });
    const tgtByCol = models.find(function (m) { return (m.columns || []).some(function (c) { return c.id === rel.target; }); });
    const srcTable = srcByCol ? srcByCol.id : (models.find(function (m) { return m.id === rel.source; }) || {}).id || rel.source;
    const tgtTable = tgtByCol ? tgtByCol.id : (models.find(function (m) { return m.id === rel.target; }) || {}).id || rel.target;
    /* For table-level connections (ACL), use the item id as both source and sourceHandle */
    const srcHandle = srcByCol ? rel.source : srcTable;
    const tgtHandle = tgtByCol ? rel.target : tgtTable;
    return { ...rel, sourceTable: srcTable, targetTable: tgtTable, source: srcHandle, target: tgtHandle };
  });
  return { models, connections };
}

/* ---- Dagre layout ---- */
const NODE_WIDTH = 260;
const BASE_HEIGHT = 44;
const ROW_HEIGHT = 40;

function layoutElements(nodes, edges, direction, showColumns) {
  const sc = showColumns !== false;
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(function () { return {}; });
  g.setGraph({ rankdir: direction, nodesep: 60, ranksep: 80 });
  const isH = direction === 'LR';
  nodes.forEach(function (n) {
    const colCount = sc ? (n.data.columns || []).length : 0;
    g.setNode(n.id, { width: NODE_WIDTH, height: BASE_HEIGHT + colCount * ROW_HEIGHT });
  });
  edges.forEach(function (e) { g.setEdge(e.source, e.target); });
  dagre.layout(g);
  return nodes.map(function (n) {
    const pos = g.node(n.id);
    const colCount = sc ? (n.data.columns || []).length : 0;
    const nh = BASE_HEIGHT + colCount * ROW_HEIGHT;
    return {
      ...n,
      data: { ...n.data, _showColumns: sc },
      targetPosition: isH ? Position.Left : Position.Top,
      sourcePosition: isH ? Position.Right : Position.Bottom,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - nh / 2 }
    };
  });
}

/* ---- Header color by display type ---- */
const TYPE_COLORS = {
  table: '#1565c0', user: '#2e7d32', group: '#e65100', sa: '#4527a0',
  domain: '#00695c', role: '#c62828', 'rls-role': '#ad1457'
};
const TYPE_ICONS = {
  user: '\u{1F464}', group: '\u{1F465}', sa: '\u{2699}', domain: '\u{1F310}',
  role: '\u{1F6E1}', 'rls-role': '\u{1F512}'
};

/* ---- Custom node component ---- */
function ModelNode({ data }) {
  const dt = data.displayType || 'table';
  const headerBg = data.isTask ? '#7b1fa2' : (TYPE_COLORS[dt] || '#1565c0');
  const icon = TYPE_ICONS[dt] || '';
  const sc = data._showColumns !== false;
  const hasCols = sc && (data.columns || []).length > 0;
  const isH = data._direction !== 'TB';
  const tgtPos = isH ? Position.Left : Position.Top;
  const srcPos = isH ? Position.Right : Position.Bottom;

  const parts = data.id.split('.');
  const domain = (dt === 'table' && parts.length > 1) ? parts.slice(0, -1).join('.') : null;
  const tableName = data.label || parts[parts.length - 1];
  const hasDomain = !!domain;
  const headerH = hasDomain ? 48 : 36;

  /* Handle style helpers — position differs for LR vs TB */
  var hdrTgt = isH
    ? { left: -4, top: hasCols ? '20px' : '50%', width: 8, height: 8, background: headerBg }
    : { top: -4, left: '50%', width: 8, height: 8, background: headerBg };
  var hdrSrc = isH
    ? { right: -4, top: hasCols ? '20px' : '50%', width: 8, height: 8, background: headerBg }
    : { bottom: -4, left: '50%', width: 8, height: 8, background: headerBg };
  var colTgt = isH
    ? { left: -4, width: 8, height: 8, background: '#1565c0' }
    : { top: -4, left: '30%', width: 8, height: 8, background: '#1565c0' };
  var colSrc = isH
    ? { right: -4, width: 8, height: 8, background: '#1565c0' }
    : { bottom: -4, left: '70%', width: 8, height: 8, background: '#1565c0' };

  return h('div', { style: { background: '#fff', border: '1px solid #ccc', borderRadius: 6, width: NODE_WIDTH, fontSize: 13, fontFamily: "'SF Mono','Fira Code',monospace", overflow: 'hidden', boxShadow: '0 2px 6px rgba(0,0,0,.1)' } },
    /* header — always render table-level handles so ACL/table-level edges can connect */
    h('div', { style: { background: headerBg, color: '#fff', padding: hasDomain ? '4px 10px' : '8px 10px', position: 'relative', minHeight: headerH } },
      h(Handle, { type: 'target', position: tgtPos, id: data.id, style: hdrTgt }),
      hasDomain
        ? h('div', null,
          h('div', { style: { fontSize: 11, opacity: .7 } }, domain),
          h('div', { style: { fontWeight: 700, fontSize: 13 } }, tableName))
        : h('div', { style: { fontWeight: 600, display: 'flex', alignItems: 'center', gap: 6 } },
          icon && h('span', { style: { fontSize: 14 } }, icon),
          h('span', null, tableName)
        ),
      h(Handle, { type: 'source', position: srcPos, id: data.id, style: hdrSrc })
    ),
    /* columns (only when visible) */
    ...(sc ? (data.columns || []) : []).map(function (col, i) {
      return h('div', {
        key: col.id,
        style: { padding: '5px 10px', borderTop: '1px solid #eee', background: i % 2 === 0 ? '#fff' : '#fafafa', position: 'relative', height: ROW_HEIGHT, display: 'flex', alignItems: 'center', gap: 4 }
      },
        col.isTarget && h(Handle, { type: 'target', position: tgtPos, id: col.id, style: colTgt }),
        col.primaryKey && h('span', { style: { color: '#888', fontWeight: 700, fontSize: 11 } }, 'PK'),
        col.foreignKey && h('span', { style: { color: '#888', fontWeight: 700, fontSize: 11 } }, 'FK'),
        h('span', null, col.name),
        col.columnType && h('span', { style: { color: '#888' } }, ':' + col.columnType),
        col.isSource && h(Handle, { type: 'source', position: srcPos, id: col.id, style: colSrc })
      );
    })
  );
}

const nodeTypes = { model: ModelNode };

/* ---- Inner Flow component (needs useReactFlow) ---- */
function FlowInner({ models, connections, direction, showColumns }) {
  const rf = useReactFlow();
  const initNodes = useMemo(function () {
    return models.map(function (m, i) { return { id: m.id, position: { x: i * 300, y: 0 }, data: { ...m, _direction: direction }, type: 'model' }; });
  }, [models, direction]);
  const buildEdges = function (sc) {
    return connections.map(function (c, i) {
      var sh = sc ? (c.source || '') : (c.sourceTable || '');
      var th = sc ? (c.target || '') : (c.targetTable || '');
      return { id: 'e' + i, source: c.sourceTable || '', target: c.targetTable || '', sourceHandle: sh, targetHandle: th, animated: true, markerEnd: { type: MarkerType.ArrowClosed }, style: { stroke: '#1565c0' } };
    });
  };
  const initEdges = useMemo(function () { return buildEdges(showColumns); }, [connections, showColumns]);
  const laid = useMemo(function () { return layoutElements(initNodes, initEdges, direction, showColumns); }, [initNodes, initEdges, direction, showColumns]);
  const [nodes, setNodes, onNodesChange] = useNodesState(laid);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initEdges);

  useEffect(function () {
    const n = layoutElements(initNodes, initEdges, direction, showColumns);
    setNodes(n);
    setEdges(buildEdges(showColumns));
    setTimeout(function () { rf.fitView({ padding: 0.15 }); }, 50);
  }, [direction, showColumns, initNodes, initEdges]);

  return h(ReactFlow, {
    nodes, edges, onNodesChange, onEdgesChange, nodeTypes,
    fitView: true, minZoom: 0.1, attributionPosition: 'bottom-left'
  },
    h(Background, { gap: 16, size: 1, color: '#e0e0e0' }),
    h(Controls, null)
  );
}

/* ---- Public mount function ---- */
export function mountFlow(container, rawData, format, initialDirection, attributeMode) {
  const diagram = format === 'lineage' ? convertLineageFormat(rawData) : convertTableDepsFormat(rawData);
  const { models, connections } = enrichSchema(diagram);
  if (models.length === 0) {
    container.innerHTML = '<p style="padding:20px;color:#616161;font-style:italic">No data available.</p>';
    return;
  }
  /* attributeMode: 'toggle' (default) = show toggle button, 'hidden' = always hide, 'visible' = always show */
  const mode = attributeMode || 'toggle';

  function App() {
    var dir = initialDirection || 'LR';
    var ref = useState(mode !== 'hidden');
    var showColumns = ref[0];
    var setShowColumns = ref[1];
    var canToggle = mode === 'toggle';
    var btnStyle = {
      position: 'absolute', top: 8, right: 8, zIndex: 10,
      padding: '4px 10px', fontSize: 12, cursor: 'pointer',
      background: showColumns ? '#1565c0' : '#fff',
      color: showColumns ? '#fff' : '#1565c0',
      border: '1px solid #1565c0', borderRadius: 4
    };
    return h(ReactFlowProvider, null,
      h('div', { style: { width: '100%', height: 600, border: '1px solid #e0e0e0', borderRadius: 8, background: '#fafafa', position: 'relative' } },
        canToggle && h('button', { style: btnStyle, onClick: function () { setShowColumns(!showColumns); } },
          showColumns ? 'Hide Attributes' : 'Show Attributes'),
        h(FlowInner, { models, connections, direction: dir, showColumns: showColumns })
      )
    );
  }

  createRoot(container).render(h(App));
}