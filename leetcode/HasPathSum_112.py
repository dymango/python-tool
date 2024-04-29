class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


"""
给你二叉树的根节点 root 和一个表示目标和的整数 targetSum 。判断该树中是否存在 根节点到叶子节点 的路径，这条路径上所有节点值相加等于目标和 targetSum 。如果存在，返回 true ；否则，返回 false 。

叶子节点 是指没有子节点的节点。
"""


class Solution:
    def hasPathSum(self, root: TreeNode, targetSum: int) -> bool:
        if root is None: return False
        if root.left is None and root.right is None:
            if targetSum == root.val:
                return True
            else:
                return False

        val = targetSum - root.val
        return self.hasPathSum(root.left, val) or self.hasPathSum(root.right, val)
