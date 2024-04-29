from typing import Optional

"""
给你一个链表的头节点 head 和一个特定值 x ，请你对链表进行分隔，使得所有 小于 x 的节点都出现在 大于或等于 x 的节点之前。

你应当 保留 两个分区中每个节点的初始相对位置。
"""


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


class Solution:
    def partition(self, head: Optional[ListNode], x: int) -> Optional[ListNode]:
        headPointer = head
        pre = ListNode(-1)
        pre.next = headPointer
        preP = pre
        while headPointer is not None:
            if headPointer.val >= x:
                tp = headPointer
                while tp.next is not None:
                    if tp.next.val < x:
                        t = tp.next
                        tp.next = tp.next.next
                        t.next = headPointer
                        preP.next = t
                        preP = preP.next
                    else:
                        tp = tp.next
                break

            preP = preP.next
            headPointer = headPointer.next
        return pre.next
