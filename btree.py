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

        #: TODO: replace with binary search
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if i < len(node.keys) and key == node.keys[i]:
            return (node, i)
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
            # TODO: replace with list.insert
            node.keys.append(0)
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
            node.keys[i + 1] = key
        else:
            # TODO: replace with binary search
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1

            if len(node.children) > i and len(node.children[i].keys) == (2 * self._order) - 1:
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

        new_node.keys = child.keys[order:((2 * order) - 1)]
        child.keys = child.keys[0:(order - 1)]

        if not child.leaf:
            new_node.children = child.children[order:(2 * order)]
            child.children = child.children[0:order]


if __name__ == "__main__":
    tree = BTree(10)
    for i in range(1000):
        tree.insert(i)

    print(tree.search(101))
    print(tree.search(-1))
    print(tree.search(1001))

