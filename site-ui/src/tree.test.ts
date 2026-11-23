import { describe, expect, it } from 'vitest';
import { databaseTree, filterTree, projectTree, type TreeNode } from './tree';
import { model } from './test/fixture';

const labels = (nodes: TreeNode[]) => nodes.map((n) => n.label);

describe('projectTree', () => {
  it('groups loads by domain, transforms by job and nodes by dag', () => {
    const tree = projectTree(model);
    expect(tree.map((n) => n.key)).toEqual(['load', 'transform', 'dags']);
    expect(labels(tree[0].children)).toEqual(['sales']);
    expect(tree[0].children[0].children.map((n) => n.nodeId)).toEqual(['load:sales.customers', 'load:sales.orders']);
    expect(labels(tree[1].children[0].children)).toEqual(['revenue', 'top_customers']);
    expect(labels(tree[2].children)).toEqual(['airflow_load', 'airflow_transform']);
  });

  it('gives every entry a unique key', () => {
    const keys: string[] = [];
    const visit = (ns: TreeNode[]) => ns.forEach((n) => { keys.push(n.key); visit(n.children); });
    visit(projectTree(model));
    expect(new Set(keys).size).toBe(keys.length);
  });

  it('tolerates absent groups', () => {
    const tree = projectTree({ ...model, groups: {} });
    expect(tree.map((n) => n.children)).toEqual([[], [], []]);
  });
});

describe('databaseTree', () => {
  it('groups by database or connection, then schema', () => {
    const tree = databaseTree(model);
    expect(labels(tree)).toEqual(['bq', 'default', 'duckdb']);
    expect(labels(tree[1].children)).toEqual(['refdata']);
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
