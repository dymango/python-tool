import argparse

def main():
    # 创建一个 ArgumentParser 对象
    parser = argparse.ArgumentParser(description='这是一个命令行参数解析示例。')

    # 添加命令行参数
    parser.add_argument('action')
    parser.add_argument('-k', '--key')
    parser.add_argument('-v', '--value')

    # 添加命令行选项（也称为标志）


    # 解析命令行参数和选项
    args = parser.parse_args()

    # 使用命令行参数和选项
    print(args.action, args.key, args.value)


if __name__ == '__main__':
    main()
