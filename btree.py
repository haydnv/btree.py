import bisect
import math

from collections import deque

# sources:
#   https://gist.github.com/natekupp/1763661 (assumes, but does not enforce, unique keys)
#   https://en.wikipedia.org/wiki/B-tree

# note that this is not strictly a B-tree because leaves can exist at different levels
# due to the simpler (but less efficient) deletion/rebalancing algorithm

class _BTreeKey(object):
    def __init__(self, value):
        if value != list(value):
            print("{} != {}".format(value, list(value)))
            raise ValueError

        self.value = list(value)
        self.deleted = False

    def __eq__(self, other):
        return list(self) == list(other)

    def __getitem__(self, i):
        return self.value[i]

    def __list__(self):
        return self.value

    def __len__(self):
        return len(self.value)

    def __gt__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] > other[i]:
                return True

        return False

    def __ge__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] >= other[i]:
                return True

        return False

    def __lt__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] < other[i]:
                return True

        return False

    def __le__(self, other):
        for i in range(min(len(self), len(other))):
            if self[i] <= other[i]:
                return True

        return False

    def __str__(self):
        return str(self.value)


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

    def _slice(self, start, end):
        if self.leaf:
            l = bisect.bisect_left(self.keys, start)
            r = bisect.bisect(self.keys, end)
            yield from ((self, i) for i in range(l, r))
        elif end < self.keys[0]:
            yield from self.children[0]._slice(start, end)
        elif start > self.keys[-1]:
            yield from self.children[-1]._slice(start, end)
        else:
            l = bisect.bisect_left(self.keys, start)
            r = bisect.bisect(self.keys, end)

            for i in range(l, r):
                yield from self.children[i]._slice(start, end)
                yield (self, i)

            yield from self.children[r]._slice(start, end)

    def select_all(self):
        if self.leaf:
            for k in self.keys:
                if not k.deleted:
                    yield list(k)
        else:
            for i in range(len(self.keys)):
                yield from self.children[i].select_all()
                if not self.keys[i].deleted:
                    yield list(self.keys[i])

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

    def select_all(self):
        return self._root.select_all()

    def select(self, start, end = None, node = None):
        if end is None:
            end = start
        if node is None:
            node = self._root

        for (node, i) in self._root._slice(start, end):
            if not node.keys[i].deleted:
                yield list(node.keys[i])

    def delete(self, key):
        for (node, i) in self._root._slice(key, key):
            node.keys[i].deleted = True
            node.has_deleted_key = True

    def rebalance(self):
        self._root = self._rebalance(self._root)

    def _rebalance(self, node):
        valid_children = True
        for i in range(len(node.children)):
            node.children[i] = self._rebalance(node.children[i])
            if not node.children[i].valid(self._order):
                valid_children = False

        if valid_children and node.valid(self._order):
            return node
        else:
            new_node = BTree(self._order)
            for key in node.select_all():
                new_node.insert(key)
            return new_node._root

    def insert(self, key):
        key = _BTreeKey(key)
        if len(self._root.keys) >= (2 * self._order) - 1:
            node = self._root
            self._root = _BTreeNode()
            self._root.children.insert(0, node)
            self._split_child(self._root, 0)

        self._insert(self._root, key)

    def _insert(self, node, key):
        i = bisect.bisect_left(node.keys, key)
        if node.leaf:
            if i < len(node.keys) and node.keys[i] == key:
                if node.keys[i].deleted:
                    node.keys[i].deleted = False
                else:
                    pass
            else:
                node.keys.insert(i, key)
        else:
            if len(node.children[i].keys) == (2 * self._order) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            self._insert(node.children[i], key)

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

