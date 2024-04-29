class Solution:
    def twoSum(self, nums: list[int], target: int) -> list[int]:
        pointer = {}
        length = len(nums)
        for i in reversed(range(0, length)):
            if nums[i] not in pointer:
                pointer[nums[i]] = i

        for i in range(0, length):
            remained = target - nums[i]
            if remained in pointer and i != pointer[remained]:
                return [i, pointer[remained]]

        return []


"""
[-1,-2,-3,-4,-5]
"""
Solution().twoSum([-1, -2, -3, -4, -5], -8)
