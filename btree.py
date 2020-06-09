import bisect
import math

from collections import deque

# sources:
#   https://gist.github.com/natekupp/1763661 (assumes, but does not enforce, unique keys)
#   https://en.wikipedia.org/wiki/B-tree

class _BTreeNode(object):
    def __init__(self, leaf = False):
        self.leaf = leaf
        self.keys = []
        self.children = []

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

    def search(self, key, node = None):
        if key is None:
            raise ValueError

        if node is None:
            node = self._root

        if not node.keys and not node.children:
            return []
        elif not node.keys:
            return self.search(key, node.children[0])

        i = bisect.bisect_left(node.keys, key)

        if i < len(node.keys) and key == node.keys[i]:
            found = []

            while i < len(node.keys) and key == node.keys[i]:
                found.append((node, i))
                if node.children:
                    found.extend(self.search(key, node.children[i]))
                i += 1

            if node.children and i == len(node.keys):
                found.extend(self.search(key, node.children[i]))

            return found
        elif node.leaf:
            return []
        else:
            return self.search(key, node.children[i])

    def select(self, start, end):
        # TODO: implement
        raise NotImplementedError

    def insert(self, key):
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

    def iter(self, node = None):
        if node is None:
            node = self._root

        if node.leaf:
            for key in node.keys:
                yield key
        else:
            for i in range(len(node.keys)):
                for key in self.iter(node.children[i]):
                    yield key
                yield node.keys[i]

            for key in self.iter(node.children[-1]):
                yield key

    def validate(self):
        root = self._root
        order = self._order

        print()
        print("BEGIN TREE")
        self.print_tree()
        print("END TREE")
        print()

        assert root.keys == sorted(root.keys)
        assert len(root.children) <= (2 * self._order)
        if not root.leaf:
            assert len(root.children) >= 2

        unvisited = deque(root.children)
        while unvisited:
            node = unvisited.popleft()
            assert node.keys == sorted(node.keys)
            assert len(node.keys) >= (2 * math.ceil(order / 2)) - 1
            assert len(node.children) <= (2 * self._order)
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

    def print_tree(self):
        unvisited = deque([("0", self._root)])
        while unvisited:
            (path, node) = unvisited.popleft()

            leaf = "leaf" if node.leaf else "not leaf"
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
                tree.validate()

            assert len(tree.search(key)) == 1

        for key in present:
            assert len(tree.search(key)) == 1

        for i in range(1000):
            key = random.randint(-i, i)
            if tree.search(key):
                assert key in present
            else:
                assert key not in present

    def test_duplicate_keys(tree, validate):
        key = 0
        for num_keys in range(100):
            for i in range(num_keys):
                tree.insert(key)
                if validate:
                    tree.validate()

                keys = tree.search(key)
                if len(keys) != i + 1:
                    print("{} x {}: {}".format(key, i + 1, len(keys)))
                    assert False
            key += 1

    def test_iteration(tree, validate):
        present = list(range(500))
        added = set()
        while added < set(present):
            key = random.choice(present)
            if key not in added:
                tree.insert(key)
                added.add(key)

        in_tree = sorted(list(tree.iter()))
        present = sorted(list(present))
        assert in_tree == present

    def run_test(test, order, validate = False):
        test(BTree(order), validate)

    for order in range(2, 120):
        run_test(test_search, order)
        run_test(test_duplicate_keys, order)
        run_test(test_iteration, order)
        print("pass: {}".format(order))

