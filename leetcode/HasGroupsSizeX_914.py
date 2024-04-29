class Solution:
    def hasGroupsSizeX(self, deck: list[int]) -> bool:
        countMap = {}
        for d in deck:
            if d not in countMap:
                countMap[d] = 0
            countMap[d] += 1

        length = len(deck)
        for i in range(1, length + 1):
            if length % i != 0:
                continue
            values = countMap.values()
            result = all(v % i == 0 for v in values)
            if result and i >= 2:
                return True

        return False


s = Solution()
s.hasGroupsSizeX([1, 1])
