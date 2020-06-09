import bisect


# based on https://gist.github.com/natekupp/1763661


class _BTreeNode(object):
    def __init__(self, leaf = False):
        self.leaf = leaf
        self.keys = []
        self.children = []

    def __str__(self):
        if self.leaf:
            return "Leaf BTreeNode with {0} keys\n\tK:{1}\n\tC:{2}\n".format(
                len(self.keys), self.keys, self.children)
        else:
            return "Internal BTreeNode with {0} keys, {1} children\n\tK:{2}\n\n".format(
                len(self.keys), len(self.children), self.keys, self.children)


class BTree(object):
    def __init__(self, order):
        if order < 2:
            raise ValueError

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

    def delete(self, key):
        # TODO: implement
        raise NotImplementedError

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

    def __str__(self):
        r = self._root
        return r.__str__() + '\n'.join([child.__str__() for child in r.children])


if __name__ == "__main__":
    import random

    def test_search(tree):
        present = set()
        for i in range(1000):
            key = random.randint(-i, i)
            present.add(key)

        for key in present:
            tree.insert(key)
            assert len(tree.search(key)) == 1

        for key in present:
            assert len(tree.search(key)) == 1

        for i in range(1000):
            key = random.randint(-i, i)
            if tree.search(key):
                assert key in present
            else:
                assert key not in present

    def test_duplicate_keys(tree):
        key = 0
        for num_keys in range(100):
            for i in range(num_keys):
                tree.insert(key)
                keys = tree.search(key)
                if len(keys) != i + 1:
                    print("{} x {}: {}".format(key, i + 1, len(keys)))
                    print(tree)
                    exit()
            key += 1

    def run_test(test, order):
        test(BTree(order))

    for order in range(2, 120):
        run_test(test_search, order)
        run_test(test_duplicate_keys, order)
        print("pass: {}".format(order))

