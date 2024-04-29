import tkinter as tk
from tkinter import messagebox
from tkinter import ttk

def submit():
    input_text = entry.get()
    if len(input_text) == 0:
        messagebox.showinfo("提示", "请输入付款申请批次号！")
        return

    selected_value = combobox.get()
    print("选择的值是:", selected_value)
    host = "80.xxx.xxx.xxx"
    password = "testpassword"
    if selected_value == "正式服":
        messagebox.showinfo("提示", "当前正式服数据库，请谨慎操作！！！")
        messagebox.showinfo("免责声明", "产生的一切后果，由您自行承担！！！")
        host = "10.xxx.xxx.xxx"
        password = "prodpassword"
    else:
        host = "80.xxx.xxx.xxx"
        password = "testpassword"

    print("您输入的文本是:", input_text)


window = tk.Tk()
window.title("删除付款申请-小姐姐定制版")

# 获取屏幕宽度和高度
screen_width = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()
# 设置窗口尺寸和位置
window_width = 400
window_height = 200
x = (screen_width - window_width) // 2
y = (screen_height - window_height) // 2
window.geometry(f"{window_width}x{window_height}+{x}+{y}")

options = ["测试服", "正式服"]
# selected_option = tk.StringVar()
# selected_option.set(options[1])
# dropdown = ttk.OptionMenu(window, selected_option, *options)
# dropdown.pack()

combobox = ttk.Combobox(window, values=options, width=10)
combobox.current(1)  # 设置默认选中的选项
combobox.pack(pady=10)

label = tk.Label(window, text="请输入付款申请批次号:")
label.pack()  # 添加标签并设置间距

entry = tk.Entry(window)
entry.pack(pady=20)  # 垂直居中

button = tk.Button(window, text="提交", command=submit)
button.pack()

window.mainloop()
