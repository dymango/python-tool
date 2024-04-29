"""
编写一个函数来查找字符串数组中的最长公共前缀。

如果不存在公共前缀，返回空字符串 ""。



示例 1：

输入：strs = ["flower","flow","flight"]
输出："fl"
示例 2：

输入：strs = ["dog","racecar","car"]
输出：""
解释：输入不存在公共前缀。


提示：

1 <= strs.length <= 200
0 <= strs[i].length <= 200
strs[i] 仅由小写英文字母组成
"""


class Solution:
    def longestCommonPrefix(self, strs: list[str]) -> str:
        r = strs[0]
        for i in range(0, len(r)):
            s = r[0:i + 1]
            for str in strs:
                if str.startswith(s) is False:
                    return r[0:i]
        return r


a = 0
b = 15
c = 2
a, b, c = b, a, a + a
print(a)
print(b)
print(c, end='/end')
