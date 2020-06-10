import bisect
import math

from collections import deque

# sources:
#   https://gist.github.com/natekupp/1763661 (assumes, but does not enforce, unique keys)
#   https://en.wikipedia.org/wiki/B-tree

class _BTreeKey(object):
    def __init__(self, value):
        self.value = value
        self.deleted = False

    def __eq__(self, other):
        return self.value == other

    def __int__(self):
        return self.value

    def __len__(self):
        return len(self.value)

    def __gt__(self, other):
        return self.value > int(other)

    def __ge__(self, other):
        return self.value >= int(other)

    def __lt__(self, other):
        return self.value < int(other)

    def __le__(self, other):
        return self.value <= int(other)

    def __str__(self):
        return str(self.value)

class _BTreeNode(object):
    def __init__(self, leaf = False):
        self.leaf = leaf
        self.keys = []
        self.children = []

    def valid(self, order):
        if len(self.children) <= (2 * order):
            return False

        if not self.leaf:
            if len(self.children) != len(self.keys) + 1:
                return False

        if any(k.deleted for k in self.keys):
            return False

        return True

    def _slice(self, start, end):
        if not self.keys and not self.children:
            return
        elif not self.keys:
            yield from self.children[0]._slice(start, end)

        i = bisect.bisect_left(self.keys, start)

        if i < len(self.keys) and self.keys[i] == start:
            while i < len(self.keys) and self.keys[i] <= end:
                yield (self, i)
                if self.children:
                    yield from self.children[i]._slice(start, end)
                i += 1

            if self.children and i == len(self.keys):
                yield from self.children[i]._slice(start, end)

        elif self.leaf:
            return
        else:
            yield from self.children[i]._slice(start, end)

    def select_all(self):
        if self.leaf:
            for k in self.keys:
                if not k.deleted:
                    yield int(k)
        else:
            for i in range(len(self.keys)):
                yield from self.children[i].select_all()
                if not self.keys[i].deleted:
                    yield int(self.keys[i])

            yield from self.children[-1].select_all()

class BTree(object):
    def __init__(self, order):
        if order < 2:
            raise ValueError

        self._order = order
        self._root = _BTreeNode(leaf = True)

    def __len__(self):
        l = 0
        unvisited = deque([self._root])
        while unvisited:
            node = unvisited.popleft()
            l += len(node.keys)
            unvisited.extend(node.children)

        return l

    def size(self, node = None):
        if node is None:
            node = self._root

        size = len(node.keys)
        if node.children:
            size += sum([self.size(c) for c in node.children])

        return size

    def select_all(self):
        return self._root.select_all()

    def select(self, start, end = None, node = None):
        if end is None:
            end = start
        if node is None:
            node = self._root

        for (node, i) in self._root._slice(start, end):
            if not node.keys[i].deleted:
                yield int(node.keys[i])

    def delete(self, key):
        for (node, i) in self._root._slice(key, key):
            node.keys[i].deleted = True

    def rebalance(self):
        self._root = self._rebalance(self._root)

    def _rebalance(self, node):
        for i in range(len(node.children)):
            node.children[i] = self._rebalance(node.children[i])

        if node.valid(self._order):
            return node
        else:
            new_node = BTree(self._order)
            for key in node.select_all():
                new_node.insert(key)
            return new_node._root

    def insert(self, key):
        key = _BTreeKey(key)
        node = self._root
        if len(node.keys) >= (2 * self._order) - 1:
            self._root = _BTreeNode()
            self._root.children.insert(0, node)
            self._split_child(self._root, 0)

        self._insert(self._root, key)

    def _insert(self, node, key):
        i = bisect.bisect_left(node.keys, key)
        if node.leaf:
            node.keys.insert(i, key)
        else:
            if len(node.children[i].keys) == (2 * self._order) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            self._insert(node.children[i], key  )

    def _split_child(self, node, i):
        order = self._order
        child = node.children[i]
        new_node = _BTreeNode(child.leaf)

        node.children.insert(i + 1, new_node)
        node.keys.insert(i, child.keys[order - 1])

        new_node.keys = child.keys[order:]
        child.keys = child.keys[0:(order - 1)]

        if not child.leaf:
            new_node.children = child.children[order:]
            child.children = child.children[:order]


def assert_valid(tree):
    root = tree._root
    order = tree._order

    print()
    print("BEGIN TREE")
    print_tree(tree)
    print("END TREE")
    print()

    assert root.keys == sorted(root.keys)
    assert len(root.children) <= (2 * order)
    if not root.leaf:
        assert len(root.children) >= 2

    unvisited = deque(root.children)
    while unvisited:
        node = unvisited.popleft()
        assert node.keys
        assert node.keys == sorted(node.keys)
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

def print_tree(tree):
    unvisited = deque([("0", tree._root)])
    while unvisited:
        (path, node) = unvisited.popleft()

        leaf = "leaf" if node.leaf else "internal"
        print("[{}]: {} keys, {} children ({}):".format(path, len(node.keys), len(node.children), leaf))
        print("\t{}".format(", ".join([str(k) for k in node.keys])))
        print()

        for i in range(len(node.children)):
            unvisited.append(("{}-{}".format(path, i), node.children[i]))


if __name__ == "__main__":
    import random

    def test_search(tree, validate):
        present = set()
        for i in range(1000):
            key = random.randint(-i, i)
            present.add(key)

        for key in present:
            tree.insert(key)
            if validate:
                assert_valid(tree)

            assert len(list(tree.select(key))) == 1

        for key in present:
            assert len(list(tree.select(key))) == 1

        for i in range(1000):
            key = random.randint(-i, i)
            if list(tree.select(key)):
                assert key in present
            else:
                assert key not in present

    def test_duplicate_keys(tree, validate):
        key = 0
        for num_keys in range(100):
            for i in range(num_keys):
                tree.insert(key)
                if validate:
                    assert_valid(tree)

                keys = list(tree.select(key))
                if len(keys) != i + 1:
                    print("{} x {}: {}".format(key, i + 1, len(keys)))
                    assert False
            key += 1

    def test_iteration(tree, validate):
        present = list(i for i in range(500))
        added = set()
        while added < set(present):
            key = random.choice(present)
            if key not in added:
                tree.insert(key)
                added.add(key)

        in_tree = sorted(list(tree.select_all()))
        present = sorted(list(present))
        assert in_tree == present

    def test_delete(tree, validate):
        for i in range(-100, 100):
            tree.insert(i)

        for i in range(100):
            key = random.randint(-100, 100)
            tree.delete(key)
            assert len(list(tree.select(key))) == 0

        contents = list(tree.select_all())
        tree.rebalance()
        if validate:
            assert_valid(tree)
        assert contents == list(tree.select_all())

        for key in tree.select_all():
            tree.delete(key)
        tree.insert(1)
        tree.rebalance()
        if validate:
            assert_valid(tree)

        assert list(tree.select_all()) == [1]
        assert tree.size() == 1

        for i in range(1, 100):
            tree.insert(i)
        for i in range(25, 50):
            tree.delete(i)
            tree.rebalance()
            if validate:
                assert_valid(tree)
            assert len(list(tree.select(i))) == 0

    def run_test(test, order, validate = False):
        test(BTree(order), validate)

    for order in range(2, 75):
        run_test(test_search, order)
        run_test(test_duplicate_keys, order)
        run_test(test_iteration, order)
        run_test(test_delete, order, True)
        print("pass: {}".format(order))

