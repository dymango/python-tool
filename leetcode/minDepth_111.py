import sys


class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


class Solution:
    def minDepth(self, root: TreeNode) -> int:
        if root is None: return 0
        deep = 1
        if root.left is None and root.right is None:
            return deep

        minVal = sys.maxsize
        if root.left is not None:
            minVal = self.minDepth(root.left)
        if root.right is not None:
            minVal = min(minVal, self.minDepth(root.right))
        return deep + minVal
