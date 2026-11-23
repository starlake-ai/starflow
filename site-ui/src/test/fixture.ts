import type { SiteModel, SiteNode, SiteStats } from '../types';

// Mirrors the real exporter output: empty values (tags, columns, warnings, ...) are absent, not [].
type Base = Pick<SiteNode, 'id' | 'kind' | 'name' | 'domain' | 'table'>;
const node = (n: Base & Partial<SiteNode>): SiteNode => n;

const nodes: SiteNode[] = [
  node({
    id: 'load:sales.customers', kind: 'load', name: 'customers', domain: 'sales', table: 'customers',
    tags: ['pii'], engine: 'duckdb', connectionRef: 'duckdb', dag: 'airflow_load',
    description: 'Customers master data',
    filePath: 'load/sales/customers.sl.yml',
    columns: [
      { name: 'id', type: 'long', sqlType: 'BIGINT', required: true, primaryKey: true },
      { name: 'email', type: 'string', required: false, primaryKey: false, description: 'Customer email' },
    ],
  }),
  node({
    id: 'load:sales.orders', kind: 'load', name: 'orders', domain: 'sales', table: 'orders',
    engine: 'duckdb', connectionRef: 'duckdb', dag: 'airflow_load',
    columns: [
      { name: 'id', type: 'long', required: false, primaryKey: false },
      {
        name: 'customer_id', type: 'long', required: false, primaryKey: false,
        foreignKey: 'customers.id', foreignKeyNode: 'load:sales.customers',
      },
      { name: 'amount', type: 'decimal', required: false, primaryKey: false },
    ],
    warnings: ["Unknown doc block 'missing_block'"],
  }),
  node({
    id: 'transform:kpi.revenue', kind: 'transform', name: 'revenue', domain: 'kpi', table: 'revenue',
    tags: ['finance'], engine: 'bigquery', connectionRef: 'bq', dag: 'airflow_transform',
    filePath: 'transform/kpi/revenue.sql',
    sql: {
      raw: 'SELECT c.id, SUM(o.amount) FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id',
    },
  }),
  node({
    id: 'transform:kpi.top_customers', kind: 'transform', name: 'top_customers', domain: 'kpi',
    table: 'top_customers', engine: 'bigquery', connectionRef: 'bq', dag: 'airflow_transform',
  }),
  node({ id: 'external:refdata.fx_rates', kind: 'external', name: 'fx_rates', domain: 'refdata', table: 'fx_rates' }),
];

export const model: SiteModel = {
  schemaVersion: 1,
  generatedAt: '2026-11-23T00:00:00Z',
  starlakeVersion: 'test',
  project: { name: 'fixture', env: 'DUCKDB', defaultConnection: 'duckdb', overview: '# Fixture' },
  nodes: Object.fromEntries(nodes.map((n) => [n.id, n])),
  edges: [
    { from: 'load:sales.orders', to: 'transform:kpi.revenue', kind: 'lineage' },
    { from: 'load:sales.customers', to: 'transform:kpi.revenue', kind: 'lineage' },
    { from: 'external:refdata.fx_rates', to: 'transform:kpi.revenue', kind: 'lineage' },
    { from: 'transform:kpi.revenue', to: 'transform:kpi.top_customers', kind: 'lineage' },
    { from: 'load:sales.orders', to: 'load:sales.customers', kind: 'fk' },
  ],
  columnEdges: [
    { from: 'load:sales.customers#id', to: 'transform:kpi.revenue#customer_id', task: 'transform:kpi.revenue' },
  ],
  groups: {
    domains: [{ name: 'sales', description: 'Sales domain', nodes: ['load:sales.customers', 'load:sales.orders'] }],
    jobs: [{ name: 'kpi', nodes: ['transform:kpi.revenue', 'transform:kpi.top_customers'] }],
    dags: [
      { name: 'airflow_load', template: 'load.py.j2', nodes: ['load:sales.customers', 'load:sales.orders'] },
      {
        name: 'airflow_transform', template: 'transform.py.j2',
        nodes: ['transform:kpi.revenue', 'transform:kpi.top_customers'],
      },
    ],
  },
  docs: { __overview__: '# Fixture' },
};

export const stats: SiteStats = {
  schemaVersion: 1,
  generatedAt: '2026-11-23T00:00:00Z',
  connections: {
    duckdb: { engine: 'duckdb', status: 'ok', durationMs: 10 },
    bq: { engine: 'bigquery', status: 'error', error: 'no credentials', durationMs: 5 },
  },
  tables: {
    'load:sales.customers': {
      connectionRef: 'duckdb', exists: true, rowCount: 120345, rowCountEstimated: true, bytes: 9123456,
      columnDiff: [
        { name: 'id', declared: 'BIGINT', actual: 'BIGINT', status: 'match' },
        { name: 'email', declared: 'VARCHAR', actual: 'TEXT', status: 'mismatch' },
      ],
    },
    'load:sales.orders': { connectionRef: 'duckdb', exists: false },
  },
};
