import bisect
import math

from collections import deque

# sources:
#   https://gist.github.com/natekupp/1763661 (assumes, but does not enforce, unique keys)
#   https://en.wikipedia.org/wiki/B-tree

# note that this is not strictly a B-tree because leaves can exist at different
# levels due to the simpler (but less efficient) deletion/rebalancing algorithm


class _BTreeKey(object):
    def __init__(self, key):
        self.key = tuple(key)
        self.deleted = False

        assert key == self.key

    def __eq__(self, other):
        return tuple(self) == tuple(other)

    def __getitem__(self, index):
        return self.key[index]

    def __ge__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] >= other[i]:
                return True

        return False

    def __gt__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] > other[i]:
                return True
            elif self[i] < other[i]:
                return False

        return False

    def __le__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] <= other[i]:
                return True

        return False

    def __len__(self):
        return len(self.key)

    def __lt__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] < other[i]:
                return True
            elif self[i] > other[i]:
                return False

        return False

    def __str__(self):
        return str(self.key)

    def __tuple__(self):
        return self.key


class _BTreeNode(object):
    def __init__(self, parent, leaf = False):
        self.parent = parent
        self.leaf = leaf
        self.keys = []
        self.children = []
        self.rebalance = False

    def __iter__(self):
        yield from self.select(slice(None))

    def __getitem__(self, index):
        yield from self.select(index)

    def select(self, bounds, reverse=False):
        rows = (
            node.keys[i] for (node, i) in self._slice(bounds, reverse)
            if not node.keys[i].deleted)
        yield from (row.key for row in rows)

    def valid(self, order):
        if self.rebalance:
            return False

        if not self.keys:
            return False

        if not self.leaf:
            if any(c.rebalance for c in self.children):
                return False

            if len(self.children) > order:
                return False
            elif len(self.children) <= math.ceil(order / 2):
                return False

            if len(self.keys) != len(self.children) - 1:
                return False

        return True

    def _slice(self, bounds, reverse):
        if isinstance(bounds, slice):
            l = bisect.bisect_left(self.keys, bounds.start) if bounds.start else 0
            r = bisect.bisect_left(self.keys, bounds.stop) if bounds.stop else len(self.keys)
            if bounds.step and bounds.step != 1:
                raise IndexError
        else:
            l = bisect.bisect_left(self.keys, bounds)
            r = bisect.bisect_right(self.keys, bounds)

        if self.leaf:
            r = reversed(range(l, r)) if reverse else range(l, r)
            yield from ((self, i) for i in r)
        else:
            if reverse:
                yield from self.children[r]._slice(bounds, reverse)

                for i in reversed(range(l, r)):
                    yield (self, i)
                    yield from self.children[i]._slice(bounds, True)
            else:
                for i in range(l, r):
                    yield from self.children[i]._slice(bounds, False)
                    yield (self, i)

                yield from self.children[r]._slice(bounds, reverse)


class BTree(object):
    def __init__(self, order, schema, keys=[]):
        assert order >= 2
        assert schema and schema == tuple(schema)

        self._len = 0
        self._order = order
        self._schema = schema
        self._root = _BTreeNode(None, leaf = True)

        for key in keys:
            self.insert(key)

        self._rebalance_queue = []

    def __getitem__(self, index):
        yield from self._root[index]

    def __delitem__(self, index):
        for (node, i) in self._root._slice(index, False):
            if not node.keys[i].deleted:
                node.keys[i].deleted = True
                node.rebalance = True
                if not node in self._rebalance_queue:
                    self._rebalance_queue.append(node)

                self._len -= 1

    def __iter__(self):
        yield from self._root

    def __len__(self):
        return self._len

    def __str__(tree):
        unvisited = deque([("0", tree._root)])
        as_str = ""
        while unvisited:
            (path, node) = unvisited.popleft()

            leaf = "leaf" if node.leaf else "internal"
            as_str += "[{}]: {} keys, {} children ({}):".format(
                path, len(node.keys), len(node.children), leaf)
            as_str += "\t{}\n".format(", ".join([str(k) for k in node.keys]))

            for i in range(len(node.children)):
                unvisited.append(("{}-{}".format(path, i), node.children[i]))

        return as_str

    def contains(self, item):
        for _ in self[item]:
            return True

        return False

    def insert(self, key):
        assert len(key) == len(self._schema)

        key = tuple(self._schema[i](key[i]) for i in range(len(self._schema)))

        key = _BTreeKey(key)
        if len(self._root.keys) >= (2 * self._order) - 1:
            node = self._root
            self._root = _BTreeNode(None)
            node.parent = self._root
            self._root.children.insert(0, node)
            self._split_child(self._root, 0)

        self._insert(self._root, key)

    def select(self, bounds, reverse=False):
        if bounds.step:
            raise IndexError

        return self._root.select(bounds, reverse)

    def rebalance(self):
        while self._rebalance_queue:
            node = self._rebalance_queue.pop()
            if node.parent is None:
                self._root = self._rebalance(node)
            elif node.valid(self._order):
                for i in range(len(node.children)):
                    if not node.children[i].valid(self._order):
                        node.children[i] = self._rebalance(node.children[i])
                        node.children[i].parent = node
                        if not node.children[i].valid(self._order):
                            node.rebalance = True

                if node.rebalance:
                    self._rebalance_queue.append(node.parent)

            elif node.parent is None:
                self._root = self._rebalance(node)
            else:
                self._rebalance_queue.append(node.parent)

    def _insert(self, node, key):
        i = bisect.bisect_left(node.keys, key)
        if node.leaf:
            if i < len(node.keys) and node.keys[i] == key:
                if node.keys[i].deleted:
                    node.keys[i].deleted = False
                    self._len += 1
                else:
                    pass
            else:
                node.keys.insert(i, key)
                self._len += 1
        else:
            if len(node.children[i].keys) == (2 * self._order) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            self._insert(node.children[i], key)

    def _rebalance(self, node):
        return BTree(self._order, self._schema, node[:])._root

    def _split_child(self, node, i):
        order = self._order
        child = node.children[i]
        new_node = _BTreeNode(node, child.leaf)

        node.children.insert(i + 1, new_node)
        node.keys.insert(i, child.keys[order - 1])

        new_node.keys = child.keys[order:]
        child.keys = child.keys[0:(order - 1)]

        if not child.leaf:
            new_node.children = child.children[order:]
            for node in new_node.children:
                node.parent = new_node

            child.children = child.children[:order]
            for node in child.children:
                node.parent = child

