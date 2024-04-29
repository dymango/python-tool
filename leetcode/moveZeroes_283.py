"""
给定一个数组 nums，编写一个函数将所有 0 移动到数组的末尾，同时保持非零元素的相对顺序。

请注意 ，必须在不复制数组的情况下原地对数组进行操作。



示例 1:

输入: nums = [0,1,0,3,12]
输出: [1,3,12,0,0]
示例 2:

输入: nums = [0]
输出: [0]

[0,1,0,3,12]

添加到测试用例
输出
[12,1,3,0,0]
预期结果
[1,3,12,0,0]
"""
from typing import List


class Solution:
    def moveZeroes(self, nums: List[int]) -> None:
        """
        Do not return anything, modify nums in-place instead.
        """
        zero = 0
        nonZero = 0
        length = len(nums)
        while zero < length and nums[zero] != 0:
            zero += 1

        while nonZero < length and zero < length:
            if nums[nonZero] != 0 and nonZero > zero:
                nums[zero] = nums[nonZero]
                nums[nonZero] = 0
                while zero < length and nums[zero] != 0:
                    zero += 1

            nonZero = nonZero + 1
