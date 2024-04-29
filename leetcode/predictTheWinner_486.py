"""
给你一个整数数组 nums 。玩家 1 和玩家 2 基于这个数组设计了一个游戏。
玩家 1 和玩家 2 轮流进行自己的回合，玩家 1 先手。开始时，两个玩家的初始分值都是 0 。每一回合，玩家从数组的任意一端取一个数字（即，nums[0] 或 nums[nums.length - 1]），取到的数字将会从数组中移除（数组长度减 1 ）。
玩家选中的数字将会加到他的得分上。当数组中没有剩余数字可取时，游戏结束。
如果玩家 1 能成为赢家，返回 true 。如果两个玩家得分相等，同样认为玩家 1 是游戏的赢家，也返回 true 。你可以假设每个玩家的玩法都会使他的分数最大化。


示例 1：
输入：nums = [1,5,2]
输出：false
解释：一开始，玩家 1 可以从 1 和 2 中进行选择。
如果他选择 2（或者 1 ），那么玩家 2 可以从 1（或者 2 ）和 5 中进行选择。如果玩家 2 选择了 5 ，那么玩家 1 则只剩下 1（或者 2 ）可选。
所以，玩家 1 的最终分数为 1 + 2 = 3，而玩家 2 为 5 。
因此，玩家 1 永远不会成为赢家，返回 false 。

示例 2：
输入：nums = [1,5,233,7]
输出：true
解释：玩家 1 一开始选择 1 。然后玩家 2 必须从 5 和 7 中进行选择。无论玩家 2 选择了哪个，玩家 1 都可以选择 233 。
最终，玩家 1（234 分）比玩家 2（12 分）获得更多的分数，所以返回 true，表示玩家 1 可以成为赢家。
提示：

1 <= nums.length <= 20
0 <= nums[i] <= 107

为了判断哪个玩家可以获胜，需要计算一个总分，即先手得分与后手得分之差。当数组中的所有数字都被拿取时，如果总分大于或等于 000，则先手获胜，反之则后手获胜。
由于每次只能从数组的任意一端拿取数字，因此可以保证数组中剩下的部分一定是连续的。假设数组当前剩下的部分为下标 start\textit{start}start 到下标 end\textit{end}end，其中 0≤start≤end<nums.length0 \le \textit{start} \le \textit{end} < \textit{nums}.\text{length}0≤start≤end<nums.length。如果 start=end\textit{start}=\textit{end}start=end，则只剩一个数字，当前玩家只能拿取这个数字。如果 start<end\textit{start}<\textit{end}start<end，则当前玩家可以选择 nums[start]\textit{nums}[\textit{start}]nums[start] 或 nums[end]\textit{nums}[\textit{end}]nums[end]，然后轮到另一个玩家在数组剩下的部分选取数字。这是一个递归的过程。
计算总分时，需要记录当前玩家是先手还是后手，判断当前玩家的得分应该记为正还是负。当数组中剩下的数字多于 111 个时，当前玩家会选择最优的方案，使得自己的分数最大化，因此对两种方案分别计算当前玩家可以得到的分数，其中的最大值为当前玩家最多可以得到的分数。

作者：力扣官方题解
链接：https://leetcode.cn/problems/predict-the-winner/solutions/395940/yu-ce-ying-jia-by-leetcode-solution/
来源：力扣（LeetCode）
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
"""
from typing import List


class Solution:
    def predictTheWinner(self, nums: List[int]) -> bool:
        return self.canWin(nums, 0, len(nums) - 1, 0, 0, 0)

    def canWin(self, nums: List[int], start, end, player1, player2, round):
        if start > end: return player1 >= player2
        win = False
        if round % 2 == 0:
            player1 += nums[start]
        else:
            player2 += nums[start]

        win |= self.canWin(nums, start + 1, end, player1, player2, round + 1)
        if round % 2 == 0:
            player1 -= nums[start]
        else:
            player2 -= nums[start]

        if round % 2 == 0:
            player1 += nums[end]
        else:
            player2 += nums[end]

        return win or self.canWin(nums, start, end - 1, player1, player2, round + 1)


# print(Solution().predictTheWinner([1, 5, 2]))
print(Solution().predictTheWinner([1, 5, 233, 7]))
