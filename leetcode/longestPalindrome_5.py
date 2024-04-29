"""
给你一个字符串 s，找到 s 中最长的回文子串。
如果字符串的反序与原始字符串相同，则该字符串称为回文字符串。


示例 1：

输入：s = "babad"
输出："bab"
解释："aba" 同样是符合题意的答案。
示例 2：

输入：s = "cbbd"
输出："bb"


提示：

1 <= s.length <= 1000
s 仅由数字和英文字母组成
"""


class Solution:
    def longestPalindrome(self, s: str) -> str:
        length = len(s)
        maxL = 0
        resultStr = ""
        arr = [[None]*length for _ in range(length)]
        for winWidth in range(0, length):
            for winStart in range(0, length):
                start = winStart - winWidth
                end = winStart + winWidth
                if start < 0 or end >= length: continue
                if s[start] == s[end]:
                    if start + 1 >= end - 1 or arr[start + 1][end - 1]:
                        arr[start][end] = True
                        if end - start + 1 > maxL:
                            maxL = end - start + 1
                            resultStr = s[start:end + 1]
                    else:
                        arr[start][end] = False
                else:
                    arr[start][end] = False

        for winWidth in range(0, length):
            for winStart in range(0, length):
                start = winStart - winWidth
                end = winStart + 1 + winWidth
                if start < 0 or end >= length: continue
                if s[start] == s[end]:
                    if start + 1 >= end - 1 or arr[start + 1][end - 1]:
                        arr[start][end] = True
                        if end - start + 1 > maxL:
                            maxL = end - start + 1
                            resultStr = s[start:end + 1]
                    else:
                        arr[start][end] = False
                else:
                    arr[start][end] = False
        return resultStr


print(Solution().longestPalindrome("cbbd"))
