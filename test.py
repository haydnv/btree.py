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
        tree.insert([key])
        if validate:
            assert_valid(tree)

        assert len(list(tree.select([key]))) == 1

    for key in present:
        assert len(list(tree.select([key]))) == 1

    for i in range(1000):
        key = random.randint(-i, i)
        if list(tree.select([key])):
            assert key in present
        else:
            assert key not in present


def test_duplicate_keys(tree, validate):
    key = 0
    for num_keys in range(100):
        for i in range(num_keys):
            tree.insert([key])
            if validate:
                assert_valid(tree)

            assert len(list(tree.select([key]))) == 1

        key += 1


def test_iteration(tree, validate):
    present = list(i for i in range(500))
    added = set()
    while added < set(present):
        key = random.choice(present)
        if key not in added:
            tree.insert([key])
            added.add(key)

    in_tree = sorted(list(tree.select_all()))
    present = sorted([i] for i in present)
    assert in_tree == present


def test_delete(tree, validate):
    for i in range(-100, 100):
        tree.insert([i])

    for i in range(100):
        key = [random.randint(-100, 100)]
        tree.delete(key)
        assert len(list(tree.select(key))) == 0

    contents = list(tree.select_all())
    tree.rebalance()
    if validate:
        assert_valid(tree)
    assert contents == list(tree.select_all())

    for key in tree.select_all():
        tree.delete(key)
        if validate:
            assert_valid(tree)

    tree.insert([1])
    tree.rebalance()
    if validate:
        assert_valid(tree)

    assert list(tree.select_all()) == [[1]]
    assert len(tree) == 1

    for i in range(1, 100):
        tree.insert([i])
    for i in range(25, 50):
        tree.delete([i])
        tree.rebalance()
        if validate:
            assert_valid(tree)
        assert len(list(tree.select([i]))) == 0


def test_compound_keys(tree, validate):
    for i in range(10):
        for j in range(10):
            tree.insert([i, j])
            if validate:
                assert_valid(tree)

    for i in range(10):
        assert len(list(tree.select([i]))) == 10

    num_keys = 100
    for i in range(10):
        tree.delete([i])
        if validate:
            assert_valid(tree)

        num_keys -= 10
        assert len(list(tree.select_all())) == num_keys


def run_test(test, order, validate = False):
    test(BTree(order), validate)


if __name__ == "__main__":
    for order in range(2, 75):
        run_test(test_search, order)
        run_test(test_duplicate_keys, order)
        run_test(test_iteration, order)
        run_test(test_delete, order)
        run_test(test_compound_keys, order)
        print("pass: {}".format(order))

