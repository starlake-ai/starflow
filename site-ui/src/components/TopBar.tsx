import { useState } from 'react';
import type { SiteModel, SiteStats } from '../types';
import { currentTheme, toggleTheme } from '../theme';
import { graphHref } from '../router';

export function TopBar({ model, stats, onSearch }: { model: SiteModel; stats: SiteStats | null; onSearch: () => void }) {
  const [theme, setTheme] = useState(currentTheme());
  const failed = stats ? Object.values(stats.connections ?? {}).filter((c) => c.status === 'error').length : 0;
  const label = !stats ? 'no stats' : failed > 0 ? `stats: ${failed} connection(s) failed` : 'stats ok';
  return (
    <header className="topbar">
      <a className="brand" href="#/">
        Starflow
      </a>
      <span className="project">{model.project.name}</span>
      {model.project.env && <span className="pill">env {model.project.env}</span>}
      <button className="search-btn" onClick={onSearch}>
        Search <kbd>Ctrl K</kbd>
      </button>
      <a className="nav" href={graphHref()}>
        Graph
      </a>
      <span className="spacer" />
      <span
        className={`pill ${stats ? (failed ? 'warn' : 'ok') : 'muted'}`}
        title={stats ? `Stats generated ${stats.generatedAt}` : 'Run starlake site --stats to collect warehouse stats'}
      >
        {label}
      </span>
      <button className="icon" title="Toggle theme" onClick={() => setTheme(toggleTheme())}>
        {theme === 'dark' ? '☀' : '☾'}
      </button>
    </header>
  );
}
