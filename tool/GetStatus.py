import os
import psutil
import time
import sys


def print_top_processes(n=10):
    # 获取所有进程并按CPU使用率排序
    processes = psutil.process_iter(['pid', 'cpu_percent', 'memory_percent', 'name'])
    sorted_processes = sorted(processes, key=lambda proc: proc.info['cpu_percent'], reverse=True)

    # 打印头部信息
    os.system('cls' if os.name == 'nt' else 'clear')  # 清屏
    print(f"{'PID':>5} {'CPU%':>20} {'MEM%':>20} {'NAME':<20}")
    print("-" * 47)

    # 打印进程信息
    for i, proc in enumerate(sorted_processes[:n]):
        try:
            cpu_usage = proc.info['cpu_percent']
            mem_usage = proc.info['memory_percent']
            name = proc.info['name'][:17] + '...' if len(proc.info['name']) > 20 else proc.info['name']
            print(f"{proc.info['pid']:>5} {cpu_usage:>10.4f} {mem_usage:>10.2f} {name:<20}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass  # 忽略访问被拒绝或进程不存在的情况


def main():
    try:
        while True:
            print_top_processes()
            time.sleep(1)  # 每秒更新一次
    except KeyboardInterrupt:
        pass  # 捕获Ctrl+C中断并退出程序


if __name__ == "__main__":
    main()