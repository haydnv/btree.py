import math
import random

from btree import BTree
from collections import deque


def assert_valid(tree):
    root = tree._root
    order = tree._order

    print()
    print("BEGIN TREE")
    print(tree)
    print("END TREE")
    print()

    assert root.keys == sorted(list(k) for k in root.keys)
    assert len(root.children) <= (2 * order)
    if not root.leaf:
        assert len(root.children) >= 2

    unvisited = deque(root.children)
    while unvisited:
        node = unvisited.popleft()

        assert node.keys
        assert node.keys == sorted(list(k) for k in node.keys)
        assert len(node.children) <= (2 * order)
        if node.leaf:
            assert not node.children
        else:
            assert len(node.children) == len(node.keys) + 1
            assert len(node.children) >= math.ceil(order / 2)

            for i in range(len(node.keys)):
                assert node.children[i].keys
                assert node.children[i + 1].keys
                assert node.children[i].keys[-1] <= node.keys[i]
                assert node.children[i + 1].keys[0] >= node.keys[i]

        unvisited.extend(node.children)


def test_search(tree, validate):
    present = set()
    for i in range(1000):
        key = random.randint(-i, i)
        present.add(key)

    for key in present:
        tree.insert([key], [key])
        if validate:
            assert_valid(tree)

        assert len(list(tree[[key]])) == 1

    for key in present:
        assert len(list(tree[[key]])) == 1

    for i in range(1000):
        key = random.randint(-i, i)
        if list(tree[[key]]):
            assert key in present
        else:
            assert key not in present


def test_duplicate_keys(tree, validate):
    key = 0
    for num_keys in range(100):
        for i in range(num_keys):
            tree.insert([key], [key])
            if validate:
                assert_valid(tree)

            assert len(list(tree[[key]])) == 1

        key += 1


def test_iteration(tree, validate):
    present = list(i for i in range(500))
    added = set()
    while added < set(present):
        key = random.choice(present)
        if key not in added:
            tree.insert([key], [key])
            added.add(key)

    in_tree = sorted(list(tree[:]))
    present = sorted(((i,), (i,)) for i in present)
    assert in_tree == present


def test_delete(tree, validate):
    for i in range(-100, 100):
        tree.insert([i], [i])

    for i in range(100):
        key = [random.randint(-100, 100)]
        del tree[key]
        assert len(list(tree[key])) == 0

    contents = list(tree[:])
    tree.rebalance()
    if validate:
        assert_valid(tree)
    assert contents == list(tree[:])

    for (key, _val) in tree[:]:
        del tree[key]
        if validate:
            assert_valid(tree)

    assert len(tree) == 0

    tree.insert([1], [1])
    assert len(tree) == 1
    tree.rebalance()
    assert len(tree) == 1
    if validate:
        assert_valid(tree)

    assert list(tree[:]) == [((1,), (1,))]
    print(len(tree))
    assert len(tree) == 1

    for i in range(1, 100):
        tree.insert([i], [i])
    for i in range(25, 50):
        del tree[[i]]
        tree.rebalance()
        if validate:
            assert_valid(tree)
        assert len(list(tree[[i]])) == 0


def test_compound_keys(tree, validate):
    for i in range(10):
        for j in range(10):
            tree.insert([i, j], [])
            if validate:
                assert_valid(tree)

    for i in range(10):
        assert len(list(tree[[i]])) == 10

    num_keys = 100
    for i in range(10):
        del tree[[i]]
        if validate:
            assert_valid(tree)

        num_keys -= 10
        assert len(list(tree[:])) == num_keys


def run_test(test, order, schema, validate = False):
    test(BTree(order, schema), validate)


if __name__ == "__main__":
    for order in range(2, 75):
        run_test(test_search, order, ([int], [int]))
        run_test(test_duplicate_keys, order, ([int], [int]))
        run_test(test_iteration, order, ([int], [int]))
        run_test(test_delete, order, ([int], [int]))
        run_test(test_compound_keys, order, ([int, int], []))
        print("pass: {}".format(order))

