import { describe, expect, it } from 'vitest';
import { renderToString } from 'react-dom/server';
import { Overview } from './Overview';
import { model } from '../test/fixture';
import type { SiteModel } from '../types';

const m: SiteModel = structuredClone(model);
m.groups.domains![0].description = 'All **sales** tables';
m.groups.jobs![0].description = 'Key _indicators_';
m.groups.dags![0].comment = 'Runs the **loads**';
const html = renderToString(<Overview model={m} stats={null} />).replace(/<!-- -->/g, '');

describe('Overview', () => {
  it('renders domain descriptions as Markdown', () => {
    expect(html).toContain('<strong>sales</strong>');
  });
  it('lists jobs with their Markdown description', () => {
    expect(html).toContain('<em>indicators</em>');
    expect(html).toContain('href="#/graph?select=job%3Akpi"');
  });
  it('lists DAGs with their Markdown comment and template', () => {
    expect(html).toContain('<strong>loads</strong>');
    expect(html).toContain('load.py.j2');
    expect(html).toContain('href="#/graph?select=dag%3Aairflow_load"');
  });
});
