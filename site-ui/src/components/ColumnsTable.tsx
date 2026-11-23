import { useEffect, useRef, useState } from 'react';
import type { ColumnDiff, SiteColumn } from '../types';
import { nodeHref } from '../router';
import { diffIndex, extraColumns } from '../nodeView';
import { Markdown, Tags } from './common';

type Props = { columns: SiteColumn[]; diff?: ColumnDiff[]; highlight?: string; hasStats: boolean };

export function ColumnsTable({ columns, diff, highlight, hasStats }: Props) {
  const diffBy = diffIndex(diff);
  const extra = extraColumns(diff);
  return (
    <table className="columns">
      <thead>
        <tr>
          <th>Name</th>
          <th>Type</th>
          {hasStats && <th>Actual</th>}
          <th>Description</th>
          <th>Keys</th>
          <th>Flags</th>
        </tr>
      </thead>
      <tbody>
        {columns.map((c, i) => (
          <ColumnRow key={`${i}-${c.name}`} col={c} depth={0} diffBy={diffBy} highlight={highlight} hasStats={hasStats} />
        ))}
        {extra.map((d, i) => (
          <tr key={`extra-${i}-${d.name}`} className="extra">
            <td>{d.name}</td>
            <td />
            {hasStats && (
              <td>
                <code>{d.actual}</code>
              </td>
            )}
            <td className="muted">Only in the warehouse</td>
            <td />
            <td />
          </tr>
        ))}
      </tbody>
    </table>
  );
}

type RowProps = { col: SiteColumn; depth: number; diffBy: Map<string, ColumnDiff>; highlight?: string; hasStats: boolean };

function ColumnRow({ col, depth, diffBy, highlight, hasStats }: RowProps) {
  const [open, setOpen] = useState(false);
  const row = useRef<HTMLTableRowElement>(null);
  const finalName = col.rename ?? col.name;
  const isHit = depth === 0 && !!highlight && [col.name, finalName].some((n) => n.toLowerCase() === highlight.toLowerCase());
  useEffect(() => {
    if (isHit) row.current?.scrollIntoView?.({ block: 'center' });
  }, [isHit]);
  const d = depth === 0 ? diffBy.get(finalName.toLowerCase()) : undefined;
  const children = col.columns ?? [];
  return (
    <>
      <tr ref={row} className={isHit ? 'hit' : ''}>
        <td style={{ paddingLeft: 8 + depth * 16 }}>
          {children.length > 0 && (
            <button type="button" className="twisty" onClick={() => setOpen(!open)}>
              {open ? '▾' : '▸'}
            </button>
          )}
          {col.rename ? (
            <>
              {col.rename} <span className="muted">({col.name})</span>
            </>
          ) : (
            col.name
          )}
        </td>
        <td>
          <code>{col.sqlType ?? col.type}</code>
          {col.sqlType && col.sqlType !== col.type && <span className="muted"> {col.type}</span>}
        </td>
        {hasStats && (
          <td>
            {d?.actual && <code>{d.actual}</code>}
            {d?.status === 'mismatch' && (
              <span className="marker" title={`Declared ${d.declared ?? '?'}, actual ${d.actual ?? '?'}`}>
                ●
              </span>
            )}
            {d?.status === 'missingInWarehouse' && <span className="marker">missing</span>}
          </td>
        )}
        <td>
          <Markdown source={col.description} />
        </td>
        <td>
          {col.primaryKey && <span className="pill">PK</span>}
          {col.foreignKey &&
            (col.foreignKeyNode ? (
              <a className="pill" href={nodeHref(col.foreignKeyNode, col.foreignKey.split('.').pop())}>
                FK {col.foreignKey}
              </a>
            ) : (
              <span className="pill">FK {col.foreignKey}</span>
            ))}
        </td>
        <td>
          {col.required && <span className="pill">required</span>}
          {col.privacy && <span className="pill">{col.privacy}</span>}
          {col.script && (
            <span className="pill" title={col.script}>
              computed
            </span>
          )}
          <Tags tags={col.tags} />
        </td>
      </tr>
      {open &&
        children.map((c, i) => (
          <ColumnRow key={`${i}-${c.name}`} col={c} depth={depth + 1} diffBy={diffBy} highlight={highlight} hasStats={hasStats} />
        ))}
    </>
  );
}
