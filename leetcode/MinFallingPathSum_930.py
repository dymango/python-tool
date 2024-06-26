"""
给你一个二元数组 nums ，和一个整数 goal ，请你统计并返回有多少个和为 goal 的 非空 子数组。

子数组 是数组的一段连续部分。

 

示例 1：

输入：nums = [1,0,1,0,1], goal = 2
输出：4
解释：
有 4 个满足题目要求的子数组：[1,0,1]、[1,0,1,0]、[0,1,0,1]、[1,0,1]
示例 2：

输入：nums = [0,0,0,0,0], goal = 0
输出：15
 

提示：

1 <= nums.length <= 3 * 104
nums[i] 不是 0 就是 1
0 <= goal <= nums.length

来源：力扣（LeetCode）
链接：https://leetcode.cn/problems/binary-subarrays-with-sum
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
"""


class Solution:
    def numSubarraysWithSum(self, nums: list[int], goal: int) -> int:
        count = 0
        length = len(nums)
        sumArr = []
        for i, v in enumerate(nums):
            if i == 0:
                sumArr.append(nums[i])
            else:
                sumArr.append(sumArr[i - 1] + nums[i])

        cache = {0: 1}
        for index in range(length):
            count += cache.get(sumArr[index] - goal, 0)
            cache[sumArr[index]] = cache.get(sumArr[index], 0) + 1

        return count

s = Solution()
s.numSubarraysWithSum([1,0,1,0,1], 2)