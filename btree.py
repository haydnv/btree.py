import bisect

# based on https://gist.github.com/natekupp/1763661


class _BTreeNode(object):
    def __init__(self, leaf = False):
        self.leaf = leaf
        self.keys = []
        self.children = []


class BTree(object):
    def __init__(self, order):
        self._order = order
        self._root = _BTreeNode(leaf = True)

    def search(self, key, node = None):
        if node is None:
            node = self._root

        i = bisect.bisect_left(node.keys, key)

        if i < len(node.keys) and key == node.keys[i]:
            found = []
            while i < len(node.keys) and key == node.keys[i]:
                found.append((node, i))
                i += 1
            return found

        elif node.leaf:
            return None
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

    def delete(self, key):
        # TODO: implement
        raise NotImplementedError

    def _insert(self, node, key):
        i = len(node.keys) - 1
        if node.leaf:
            bisect.insort(node.keys, key)
        else:
            i = bisect.bisect(node.keys, key)

            if len(node.children[i].keys) >= (2 * self._order) - 1:
                self._split_child(node, i)

            self._insert(node.children[i], key)

    def _split_child(self, node, i):
        order = self._order
        child = node.children[i]
        new_node = _BTreeNode(child.leaf)

        node.children.insert(i + 1, new_node)
        node.keys.insert(i, child.keys[order - 1])

        new_node.keys = child.keys[order:((2 * order) - 1)]
        child.keys = child.keys[0:(order - 1)]

        if not child.leaf:
            new_node.children = child.children[order:(2 * order)]
            child.children = child.children[0:order]


if __name__ == "__main__":
    tree = BTree(10)
    for i in range(1000):
        tree.insert(i)
    tree.insert(502)
    tree.insert(502)
    tree.insert(502)
    tree.insert(2000)

    print(tree.search(101))
    print(tree.search(-1))
    print(tree.search(1001))
    print(list(tree.search(502)))
    print(tree.search(2000))

