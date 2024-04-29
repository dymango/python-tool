class Solution:
    def convertToTitle(self, columnNumber: int) -> str:
        charArr = ['', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
                   'T',
                   'U', 'V', 'W', 'X', 'Y', 'Z']

        s = ""
        while columnNumber > 0:
            if columnNumber <= 26:
                s = charArr[int(columnNumber)] + s
                columnNumber = 0
            else:
                temp = int(columnNumber % 26)
                if temp == 0: temp = 26
                temp2 = columnNumber - temp
                s = charArr[temp] + s
                columnNumber = temp2 / 26

        return s


#
solution = Solution()
print(solution.convertToTitle(701))
print(solution.convertToTitle(2147483647))

print(2147483647 % 676)
print(2147483647 % 26)
print(2147483624 / 26)
print(82595524 % 26)
print(82595500 / 26)
print(3176750 % 26)
