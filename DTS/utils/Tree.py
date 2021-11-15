class Tree(object):

    def __init__(self, data):
        self.data = data
        self.right = None
        self.left = None

    def print_tree(self, root):
        if not root:
            return root

        self.print_tree(root.left)
        print(root.data, end=' ')
        self.print_tree(root.right)

    def insert_bst(self, root, data):
        if not root:
            root = Tree(data)
            return root

        if data['cv_field'].lower() == root.data['cv_field'].lower():
            root.left = self.insert_bst(root.left, data)

        if data['cv_field'].lower() < root.data['cv_field'].lower():
            root.left = self.insert_bst(root.left, data)

        if data['cv_field'].lower() > root.data['cv_field'].lower():
            root.right = self.insert_bst(root.right, data)

        return root

    def print_leaves(self, root):
        if not root:
            return root

        if not root.left and not root.right:
            print(root.data, end=' ')
            return root

        if root.left:
            self.print_leaves(root.left)

        if root.right:
            self.print_leaves(root.right)

    def print_inorder(self, root):
        if not root:
            return root

        self.print_inorder(root.left)
        print(root.data)
        self.print_inorder(root.right)

    def construct_list(self, root):
        if root is None:
            return []
        return self.construct_list(root.left) + [root.data] + self.construct_list(root.right)
