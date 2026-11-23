import { describe, expect, it } from 'vitest';
import { databaseTree, filterTree, pathTo, projectTree, type TreeNode } from './tree';
import { model } from './test/fixture';

const labels = (nodes: TreeNode[]) => nodes.map((n) => n.label);

describe('projectTree', () => {
  it('groups loads by domain, transforms by job and nodes by dag', () => {
    const tree = projectTree(model);
    expect(tree.map((n) => n.key)).toEqual(['load', 'transform', 'external', 'dags']);
    expect(labels(tree[0].children)).toEqual(['sales']);
    expect(tree[0].children[0].children.map((n) => n.nodeId)).toEqual(['load:sales.customers', 'load:sales.orders']);
    expect(labels(tree[1].children[0].children)).toEqual(['revenue', 'top_customers']);
    expect(labels(tree[3].children)).toEqual(['airflow_load', 'airflow_transform']);
  });

  it('lists declared externals by domain and undeclared ones under a separate branch', () => {
    const ext = projectTree(model)[2];
    expect(ext.label).toBe('External');
    expect(labels(ext.children)).toEqual(['refdata', 'Undeclared']);
    expect(ext.children[0].children.map((n) => n.nodeId)).toEqual(['external:refdata.fx_rates']);
    const undeclared = ext.children[1];
    expect(labels(undeclared.children)).toEqual(['refdata', 'ghost']);
    expect(undeclared.children[0].children.map((n) => n.nodeId)).toEqual(['external:refdata.legacy_codes']);
    expect(undeclared.children[1].nodeId).toBe('external:ghost');
    const leaves = (ns: TreeNode[]): string[] => ns.flatMap((n) => (n.nodeId ? [n.nodeId] : leaves(n.children)));
    const ids = Object.values(model.nodes).filter((n) => n.kind === 'external').map((n) => n.id);
    expect(leaves([ext]).sort()).toEqual(ids.sort());
  });

  it('gives every entry a unique key', () => {
    const keys: string[] = [];
    const visit = (ns: TreeNode[]) => ns.forEach((n) => { keys.push(n.key); visit(n.children); });
    visit(projectTree(model));
    expect(new Set(keys).size).toBe(keys.length);
  });

  it('tolerates absent groups', () => {
    const tree = projectTree({ ...model, groups: {} });
    expect(tree.map((n) => n.children.map((c) => c.label))).toEqual([[], [], ['Undeclared'], []]);
  });
});

describe('databaseTree', () => {
  it('groups by database or connection, then schema', () => {
    const tree = databaseTree(model);
    expect(labels(tree)).toEqual(['bq', 'default', 'duckdb']);
    expect(labels(tree[1].children)).toEqual(['(none)', 'refdata']);
    expect(labels(tree[2].children[0].children)).toEqual(['customers', 'orders']);
  });
});

describe('filterTree', () => {
  it('keeps matching leaves and their parents', () => {
    const tree = filterTree(projectTree(model), 'cust');
    expect(labels(tree)).toEqual(['Load', 'Transform', 'DAGs']);
    expect(labels(tree[0].children[0].children)).toEqual(['customers']);
    expect(labels(tree[1].children[0].children)).toEqual(['top_customers']);
  });
  it('returns the tree unchanged for an empty query', () => {
    const tree = projectTree(model);
    expect(filterTree(tree, ' ')).toBe(tree);
  });
});

describe('pathTo', () => {
  it('returns the keys of every branch leading to a node', () => {
    expect(pathTo(projectTree(model), 'transform:kpi.revenue').sort()).toEqual(
      ['dags', 'dags/airflow_transform', 'transform', 'transform/kpi'].sort(),
    );
    expect(pathTo(projectTree(model), 'nope')).toEqual([]);
  });
});
