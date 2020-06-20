import bisect
import math

from collections import deque

# sources:
#   https://gist.github.com/natekupp/1763661 (assumes, but does not enforce, unique keys)
#   https://en.wikipedia.org/wiki/B-tree

# note that this is not strictly a B-tree because leaves can exist at different
# levels due to the simpler (but less efficient) deletion/rebalancing algorithm


class _BTreeKey(object):
    def __init__(self, key, value):
        self.key = tuple(key)
        self.value = tuple(value)
        self.deleted = False

        assert key == self.key
        assert value == self.value

    def __eq__(self, other):
        return tuple(self) == tuple(other)

    def __getitem__(self, index):
        return (self.key + self.value)[index]

    def __ge__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] >= other[i]:
                return True

        return False

    def __gt__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] > other[i]:
                return True

        return False

    def __le__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] <= other[i]:
                return True

        return False

    def __len__(self):
        return len(self.key) + len(self.value)

    def __lt__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] < other[i]:
                return True

        return False

    def __str__(self):
        return str(self.key + self.value)

    def __tuple__(self):
        return self.key + self.value


class _BTreeNode(object):
    def __init__(self, leaf = False):
        self.leaf = leaf
        self.keys = []
        self.children = []
        self.has_deleted_key = False

    def valid(self, order):
        if self.has_deleted_key:
            return False

        if self.leaf:
            if not self.keys:
                return False
        else:
            if len(self.children) > order:
                return False
            elif len(self.children) <= math.ceil(order / 2):
                return False

            if len(self.keys) != len(self.children) - 1:
                return False

        return True

    def _slice(self, index):
        if isinstance(index, slice):
            l = bisect.bisect_left(self.keys, index.start) if index.start else 0
            r = bisect.bisect_right(self.keys, index.stop) if index.stop else len(self.keys)
        else:
            l = bisect.bisect_left(self.keys, index)
            r = bisect.bisect_right(self.keys, index)

        if self.leaf:
            yield from ((self, i) for i in range(l, r))
        else:
            for i in range(l, r):
                yield from self.children[i]._slice(index)
                yield (self, i)

            yield from self.children[r]._slice(index)

    def __getitem__(self, index):
        rows = (node.keys[i] for (node, i) in self._slice(index) if not node.keys[i].deleted)
        yield from ((row.key, row.value) for row in rows)


class BTree(object):
    def __init__(self, order, schema, values=[]):
        assert order >= 2
        assert schema and schema == tuple(schema)

        self._len = 0
        self._order = order
        self._schema = schema
        self._root = _BTreeNode(leaf = True)

        for (key, value) in values:
            self.insert(key, value)
            self._len += 1

    def __getitem__(self, index):
        if not isinstance(index, slice):
            assert len(index)

        yield from self._root[index]

    def __delitem__(self, index):
        for (node, i) in self._root._slice(index):
            if not node.keys[i].deleted:
                node.keys[i].deleted = True
                node.has_deleted_key = True
                self._len -= 1

    def __len__(self):
        return self._len

    def __setitem__(self, index, value):
        assert len(value) == len(self._schema[1])
        value = [self._schema[1][i](value[i]) for i in range(len(value))]
        for (node, i) in self._root._slice(index):
            node.keys[i] = value

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

    def insert(self, key, value):
        assert len(key) == len(self._schema[0])
        assert len(value) == len(self._schema[1])

        key = tuple(self._schema[0][i](key[i]) for i in range(len(self._schema[0])))
        value = tuple(self._schema[1][i](key[i]) for i in range(len(self._schema[1])))

        key = _BTreeKey(key, value)
        if len(self._root.keys) >= (2 * self._order) - 1:
            node = self._root
            self._root = _BTreeNode()
            self._root.children.insert(0, node)
            self._split_child(self._root, 0)

        self._insert(self._root, key)

    def rebalance(self):
        self._root = self._rebalance(self._root)

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
        valid_children = True
        for i in range(len(node.children)):
            node.children[i] = self._rebalance(node.children[i])
            if not node.children[i].valid(self._order):
                valid_children = False

        if valid_children and node.valid(self._order):
            return node
        else:
            new_node = BTree(self._order, self._schema, node[:])
            return new_node._root

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

