import random
import tkinter as tk
from functools import partial
from tkinter import messagebox

elements = list()


def add_element(str):
    if str in elements:
        messagebox.showinfo("号码重复", "该号码已存在请勿重复输入: " + str)
        return False
    elements.append(str)
    return True


def remove_element(str):
    if str in elements:
        elements.remove(str)


def random_get_exist_element():
    return random.choice(elements)


def all_element():
    return ' | '.join(elements)


def add_action(entry, show_label):
    newValue = entry.get()
    if newValue == '':
        messagebox.showinfo("无效值", "空字符串无效")
        return
    result = add_element(newValue)
    if result:
        entry.delete(0, 'end')
        show_label.config(text=all_element())


def delete_action(entry, show_label):
    deleteValue = entry.get()
    remove_element(deleteValue)
    entry.delete(0, 'end')
    show_label.config(text=all_element())


def get_action():
    num = random_get_exist_element()
    messagebox.showinfo("获取号码", "获得号码:" + num)


def run():
    window = tk.Tk()
    window.title("摇号器")

    # 获取屏幕宽度和高度
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()
    # 设置窗口尺寸和位置
    window_width = 400
    window_height = 500
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2
    window.geometry(f"{window_width}x{window_height}+{x}+{y}")

    allElement = tk.Label(window, text="目前没有号码", wraplength=200)
    allElement.grid(row=4, column=0, columnspan=3)

    addLabel = tk.Label(window, text="存入号码:")
    addLabel.grid(row=1, column=0)
    addInput = tk.Entry(window)
    addInput.grid(row=1, column=1)
    addButton = tk.Button(window, text="存入", command=partial(add_action, addInput, allElement))
    addButton.grid(row=1, column=2)

    deleteLabel = tk.Label(window, text="删除号码:")
    deleteLabel.grid(row=2, column=0)
    deleteInputBox = tk.Entry(window)
    deleteInputBox.grid(row=2, column=1)
    deleteButton = tk.Button(window, text="删除", command=partial(delete_action, deleteInputBox, allElement))
    deleteButton.grid(row=2, column=2)

    getButton = tk.Button(window, text="从号码池中随机获取一个号码", command=get_action)
    getButton.grid(row=3, column=1)

    window.mainloop()


def main():
    run()


if __name__ == "__main__":
    main()
