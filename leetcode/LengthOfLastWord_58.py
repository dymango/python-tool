"""给你一个字符串 s，由若干单词组成，单词前后用一些空格字符隔开。返回字符串中 最后一个 单词的长度。

单词 是指仅由字母组成、不包含任何空格字符的最大子字符串。

 

示例 1：

输入：s = "Hello World"
输出：5
解释：最后一个单词是“World”，长度为5。
示例 2：

输入：s = "   fly me   to   the moon  "
输出：4
解释：最后一个单词是“moon”，长度为4。
示例 3：

输入：s = "luffy is still joyboy"
输出：6
解释：最后一个单词是长度为6的“joyboy”。
 

提示：

1 <= s.length <= 104
s 仅有英文字母和空格 ' ' 组成
s 中至少存在一个单词

来源：力扣（LeetCode）
链接：https://leetcode.cn/problems/length-of-last-word
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。"""


class Solution:
    def lengthOfLastWord(self, s: str) -> int:
        l = len(s)
        index = l - 1
        start = s[l - 1] != " "
        result = 0
        while index >= 0:
            if s[index] != " " and not start:
                start = True
                result += 1
            elif start and s[index] != " ":
                result += 1
            elif start and s[index] == " ":
                break

            index -= 1

        return result


s = Solution()
print(s.lengthOfLastWord("   fly me   to   the moon  "))

for i in range(10, 0, -1):
    print(i)
