import pandas as pd
import mysql.connector
from mysql.connector import Error
from typing import List
import sys
import os


def fetch_order_charge_item_data(order_ids: List[str], db_config: dict) -> pd.DataFrame:
    """
    从MySQL数据库获取指定OrderID列表的数据

    Args:
        order_ids: OrderID列表
        db_config: 数据库连接配置

    Returns:
        pandas DataFrame包含查询结果
    """
    connection = None
    try:
        # 建立数据库连接
        connection = mysql.connector.connect(
            host=db_config.get('host', 'localhost'),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', ''),
            port=db_config.get('port', 3306),
            charset='utf8mb4'
        )

        if connection.is_connected():
            print("成功连接到MySQL数据库")

        # 创建IN查询的占位符
        placeholders = ','.join(['%s'] * len(order_ids))

        # SQL查询
        query = f"""
        SELECT 
            order_id,
            order_item_id,
            order_item_taxable_subtotal,
            service_fee_taxable_subtotal,
            fast_past_fee_taxable_subtotal,
            delivery_fee_taxable_subtotal,
            small_order_fee_taxable_subtotal,
            taxable_subtotal,
            tax_rule_ids
        FROM order_charge_item_taxable_extension 
        WHERE order_id IN ({placeholders})
        ORDER BY order_id, order_item_id
        """

        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, order_ids)
        result = cursor.fetchall()

        return pd.DataFrame(result)

    except Error as e:
        print(f"数据库查询错误: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("数据库连接已关闭")


def save_to_excel(df: pd.DataFrame, output_file: str):
    """
    将DataFrame保存为Excel文件

    Args:
        df: 要保存的DataFrame
        output_file: 输出文件路径
    """
    try:
        # 创建Excel写入器
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 将数据写入Excel
            df.to_excel(writer, sheet_name='OrderChargeItemData', index=False)

            # 获取工作表并调整列宽
            worksheet = writer.sheets['OrderChargeItemData']
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).str.len().max(),
                    len(col)
                )
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)

        print(f"数据已成功导出到: {output_file}")

    except Exception as e:
        print(f"Excel导出错误: {e}")
        raise


def read_order_ids_from_file(file_path: str) -> List[str]:
    """
    从文件读取OrderID列表

    Args:
        file_path: 文件路径

    Returns:
        OrderID列表
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            order_ids = [line.strip() for line in f if line.strip()]
        return order_ids
    except Exception as e:
        print(f"读取文件错误: {e}")
        return []


def get_order_ids_from_input() -> List[str]:
    """
    从用户输入获取OrderID列表

    Returns:
        OrderID列表
    """
    print("请选择输入方式:")
    print("1. 直接输入OrderID（多个用逗号分隔）")
    print("2. 从文件读取")

    choice = input("请选择 (1 或 2): ").strip()

    if choice == "1":
        order_ids_input = input("请输入OrderID（多个用逗号分隔）: ").strip()
        order_ids = [oid.strip() for oid in order_ids_input.split(',') if oid.strip()]
    elif choice == "2":
        file_path = input("请输入文件路径: ").strip()
        order_ids = read_order_ids_from_file(file_path)
    else:
        print("无效选择，使用默认方式")
        order_ids_input = input("请输入OrderID（多个用逗号分隔）: ").strip()
        order_ids = [oid.strip() for oid in order_ids_input.split(',') if oid.strip()]

    return order_ids


def main():
    """
    主函数 - 从命令行参数读取OrderID列表并执行导出
    """
    # 数据库配置 - 请根据实际情况修改
    db_config = {
        'host': 'ftiuat-flexible-consumer-db.mysql.database.azure.com',
        'database': 'order',
        'user': 'datadog',
        'password': 'jPT8Q#gL9XLo%6ls',
    }

    # 输出文件路径
    output_file = 'order_charge_item_data.xlsx'

    try:
        # 获取OrderID列表
        if len(sys.argv) > 1:
            # 如果通过命令行参数传递
            if os.path.exists(sys.argv[1]):
                # 参数是文件路径
                order_ids = read_order_ids_from_file(sys.argv[1])
            else:
                # 参数是OrderID列表
                if ',' in sys.argv[1]:
                    order_ids = [oid.strip() for oid in sys.argv[1].split(',')]
                else:
                    order_ids = [arg.strip() for arg in sys.argv[1:]]
        else:
            # 交互式输入
            order_ids = get_order_ids_from_input()

        if not order_ids:
            print("错误: 未提供任何OrderID")
            return

        print(f"正在查询 {len(order_ids)} 个OrderID的数据...")
        print(f"OrderID列表: {order_ids}")

        # 获取数据
        df = fetch_order_charge_item_data(order_ids, db_config)

        if df.empty:
            print("未找到匹配的数据")
            return

        print(f"找到 {len(df)} 条记录")
        print("\n数据预览:")
        print(df.head())

        # 显示统计信息
        print("\n数据统计:")
        print(f"涉及订单数量: {df['order_id'].nunique()}")
        print(f"涉及订单项数量: {df['order_item_id'].nunique()}")

        # 保存到Excel
        save_to_excel(df, output_file)

    except KeyboardInterrupt:
        print("\n用户中断操作")
    except Exception as e:
        print(f"程序执行错误: {e}")


# 简化版本 - 直接在代码中指定OrderID
def export_specific_orders():
    """
    简化版本 - 直接在代码中指定要查询的OrderID
    """
    # 数据库配置
    db_config = {
        'host': 'localhost',
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database_name',
        'port': 3306
    }

    # 直接指定要查询的OrderID列表
    order_ids = ['3a186dce-bb0b-4fa6-816c-73152905354b']  # 替换为实际的OrderID

    output_file = 'specific_orders_data.xlsx'

    try:
        df = fetch_order_charge_item_data(order_ids, db_config)

        if df.empty:
            print("未找到指定OrderID的数据")
            return

        save_to_excel(df, output_file)
        print(f"成功导出 {len(df)} 条记录")

    except Exception as e:
        print(f"导出失败: {e}")


if __name__ == "__main__":
    # 使用主函数（交互式）
    main()

    # 或者使用简化版本（直接指定OrderID）
    # export_specific_orders()