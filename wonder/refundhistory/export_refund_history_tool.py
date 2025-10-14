# refund_exporter.py
import os
import logging
import sys
import time as totalTime
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import List, Dict, Any

import mysql.connector
import pandas as pd
import pytz

from models import Order, OrderIssue, OrderIssueItem, OrderItem, OrderChargeItem

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True
)

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'pool_name': 'custom_connection_pool',
    'pool_size': 10
}

output_dir = os.getenv('OUTPUT_DIR', '/app/export_refund_history')


def export_single_day(current_date: datetime):
    logging.info(f"开始处理日期: {current_date.strftime('%Y-%m-%d')}")
    conn = None
    try:
        conn = mysql.connector.connect(pool_name='custom_connection_pool')
        return process_single_day(current_date, conn)
    except mysql.connector.Error as err:
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - 数据库连接失败: {err}")
        return False
    finally:
        if conn:
            conn.close()


def process_single_day(current_date: datetime, conn):
    timezone = pytz.timezone('America/New_York')
    start_of_day_ny = timezone.localize(datetime.combine(current_date, time.min))
    end_of_day_ny = timezone.localize(datetime.combine(current_date, time.max))
    start_of_day_utc = start_of_day_ny.astimezone(pytz.UTC)
    end_of_day_utc = end_of_day_ny.astimezone(pytz.UTC)

    skip = 0
    refund_lines = []

    while True:
        with conn.cursor(dictionary=True) as cursor:
            sql = """
                SELECT id, user_id, order_channel, dining_option, created_time, status, remake_ref_order_id
                FROM `order`.orders
                WHERE created_time >= %s AND created_time <= %s
                AND brand_category = 'BLUE_APRON'
                AND order_channel IN ('BA_APP', 'BA_WEB')
                AND status in ('CANCELED', 'COMPLETE')
                LIMIT 100 OFFSET %s
            """
            cursor.execute(sql, (start_of_day_utc, end_of_day_utc, skip))
            rows = cursor.fetchall()

            if not rows:
                break

            orders = [Order(**row) for row in rows]
            for order in orders:
                lines = refund_lines_for_order(order, cursor)
                refund_lines.extend(lines)

            skip += 100

    if refund_lines:
        write_refund_csv(refund_lines, current_date)
    else:
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - 无数据")

    return True


def order_refund_history_for_forter(start_date: datetime, end_date: datetime):
    start = totalTime.time()
    dates_to_process = []

    current_date = start_date
    while current_date <= end_date:
        dates_to_process.append(current_date)
        current_date += timedelta(days=1)

    max_workers = min(7, len(dates_to_process))
    successful_days = 0
    failed_days = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_date = {
            executor.submit(export_single_day, date): date
            for date in dates_to_process
        }

        for future in as_completed(future_to_date):
            date = future_to_date[future]
            try:
                result = future.result()
                if result:
                    successful_days += 1
                else:
                    failed_days += 1
            except Exception as e:
                logging.info(f"{date.strftime('%Y-%m-%d')} - 线程执行异常: {e}")
                failed_days += 1

    end = totalTime.time()
    logging.info(f"\n=== 处理完成 ===")
    logging.info(f"总天数: {len(dates_to_process)}")
    logging.info(f"成功: {successful_days} 天")
    logging.info(f"失败: {failed_days} 天")
    logging.info(f"总耗时: {end - start: .2f} 秒")


def refund_lines_for_order(order: Order, cursor) -> List[Dict[str, Any]]:
    root_order_id = order.remake_ref_order_id or order.id
    order_issues = get_order_issues(order.id, cursor)
    refund_lines = []
    for issue in order_issues:
        issue_items = get_order_issue_items(issue.id, cursor)

        if not issue_items:
            continue

        if issue_items[0].issue_category == 'ORDER_ISSUE':
            order_items = get_order_items(root_order_id, cursor)
            order_charge_items = get_order_charge_items(root_order_id, cursor)

            for item in order_items:
                charge_item = next((ci for ci in order_charge_items if ci.order_item_id == item.id), None)
                if charge_item:
                    refund_line = create_refund_line(
                        root_order_id, issue, issue_items[0], item, charge_item
                    )
                    refund_lines.append(refund_line)
        else:
            issue_order_ids = list(set([item.issue_order_id for item in issue_items]))
            order_items = get_order_items_by_ids(issue_order_ids, cursor)
            order_charge_items = get_order_charge_items_by_ids(issue_order_ids, cursor)

            for issue_item in issue_items:
                if issue_item.issue_order_item_id:
                    order_item = next((oi for oi in order_items if oi.id == issue_item.issue_order_item_id), None)
                    charge_item = next(
                        (ci for ci in order_charge_items if ci.order_item_id == issue_item.issue_order_item_id), None)

                    if order_item and charge_item:
                        refund_line = create_refund_line(
                            root_order_id, issue, issue_item, order_item, charge_item
                        )
                        refund_lines.append(refund_line)

    return refund_lines


def get_order_issues(order_id: str, cursor) -> List[OrderIssue]:
    sql = """
    SELECT id, order_id, issue_type, created_time, discount, refund, additional_credit, concession_total
    FROM order.order_issues_v2
    WHERE order_id = %s AND issue_type IN ('COMPLAINTS', 'REMAKE')
    """

    cursor.execute(sql, (order_id,))
    rows = cursor.fetchall()
    return [OrderIssue(**row) for row in rows]


def get_order_issue_items(order_issue_id: str, cursor) -> List[OrderIssueItem]:
    sql = """
    SELECT id, order_issue_id, issue_order_id, issue_order_item_id, issue_category, issue_quantity, reason_number, issue_source
    FROM order.order_issue_items
    WHERE order_issue_id = %s AND issue_category IN ('ORDER_ISSUE', 'ITEM_ISSUE') AND issue_source IN ('CUSTOMER', 'SOCIAL')
    """

    cursor.execute(sql, (order_issue_id,))
    rows = cursor.fetchall()
    return [OrderIssueItem(**row) for row in rows]


def get_order_items(order_id: str, cursor) -> List[OrderItem]:
    sql = """
    SELECT id, menu_item_name, order_quantity, restaurant_id
    FROM order.order_items
    WHERE order_id = %s AND NOT deleted
    """

    cursor.execute(sql, (order_id,))
    rows = cursor.fetchall()
    return [OrderItem(**row) for row in rows]


def get_order_items_by_ids(order_ids: List[str], cursor) -> List[OrderItem]:
    if not order_ids:
        return []

    placeholders = ','.join(['%s'] * len(order_ids))
    sql = f"""
    SELECT id, menu_item_name, order_quantity, restaurant_id
    FROM order.order_items
    WHERE order_id IN ({placeholders}) AND NOT deleted
    """

    cursor.execute(sql, tuple(order_ids))
    rows = cursor.fetchall()
    return [OrderItem(**row) for row in rows]


def get_order_charge_items(order_id: str, cursor) -> List[OrderChargeItem]:
    sql = """
    SELECT order_item_id, subtotal, adjust_subtotal, discount, promotion, membership_subtotal, subscription_save_discount
    FROM order.order_charge_items
    WHERE order_id = %s
    """

    cursor.execute(sql, (order_id,))
    rows = cursor.fetchall()
    return [OrderChargeItem(**row) for row in rows]


def get_order_charge_items_by_ids(order_ids: List[str], cursor) -> List[OrderChargeItem]:
    if not order_ids:
        return []

    placeholders = ','.join(['%s'] * len(order_ids))
    sql = f"""
    SELECT order_item_id, subtotal, adjust_subtotal, discount, promotion, membership_subtotal, subscription_save_discount
    FROM order.order_charge_items
    WHERE order_id IN ({placeholders})
    """

    cursor.execute(sql, tuple(order_ids))
    rows = cursor.fetchall()
    return [OrderChargeItem(**row) for row in rows]


def create_refund_line(root_order_id: str, issue: OrderIssue, issue_item: OrderIssueItem,
                       order_item: OrderItem, charge_item: OrderChargeItem) -> Dict[str, Any]:
    refundable_amount = refundable_subtotal(charge_item)
    compensation_type = compensation_type_granted(issue)
    reason_cat = reason_category(issue_item.reason_number)

    return {
        "orderId": root_order_id,
        "eventTime": int(issue.created_time.timestamp() * 1000),
        "eventId": issue.id,
        "compensationStatus.itemStatus[].basicItemData.productId": "",
        "compensationStatus.itemStatus[].basicItemData.name": order_item.menu_item_name,
        "compensationStatus.itemStatus[].basicItemData.quantity": str(
            issue_item.issue_quantity if issue_item.issue_category == "ITEM_ISSUE" else order_item.order_quantity
        ),
        "compensationStatus.itemStatus[].basicItemData.category": "",
        "compensationStatus.itemStatus[].basicItemData.type": "TANGIBLE",
        "compensationStatus.itemStatus[].basicItemData.price.amountUSD": str(refundable_amount),
        "compensationStatus.itemStatus[].basicItemData.price.amountLocalCurrency": "",
        "compensationStatus.itemStatus[].basicItemData.price.currency": "",
        "compensationStatus.itemStatus[].statusData.updatedStatus": "ACCEPTED_BY_MERCHANT",
        "compensationStatus.itemStatus[].statusData.compensationTypeGranted": compensation_type,
        "compensationStatus.itemStatus[].statusData.reasonCategory": reason_cat,
        "compensationStatus.itemStatus[].statusData.internalReasonCategory": issue_item.reason_number,
        "compensationStatus.itemStatus[].statusData.returnMethodGranted": "NO_RETURN",
        "compensationStatus.itemStatus[].statusData.returnCondition": "",
        "compensationStatus.itemStatus[].statusData.statusLog.shippedByCustomerTime": "",
        "compensationStatus.itemStatus[].statusData.statusLog.arrivedToWarehouseTime": "",
        "compensationStatus.totalGrantedAmount.amountUSD": str(issue.concession_total),
        "compensationStatus.totalGrantedAmount.amountLocalCurrency": "",
        "compensationStatus.totalGrantedAmount.currency": "",
        "compensationStatus.replacementOrderId": issue.order_id if issue.issue_type == "REMAKE" else "",
        "compensationStatus.shippingRefundedAmount.amountUSD": "",
        "compensationStatus.shippingRefundedAmount.amountLocalCurrency": "",
        "compensationStatus.shippingRefundedAmount.currency": "",
        "compensationStatus.hasProofOfPurchase": ""
    }


def refundable_subtotal(charge_item: OrderChargeItem) -> Decimal:
    return (charge_item.subtotal - charge_item.adjust_subtotal - charge_item.discount -
            charge_item.promotion - charge_item.membership_subtotal - charge_item.subscription_save_discount)


def compensation_type_granted(issue: OrderIssue) -> str:
    if issue.issue_type == "REMAKE":
        return "REPLACEMENT"
    elif issue.issue_type == "COMPLAINTS":
        if (issue.discount > 0 or issue.refund > 0) and issue.additional_credit == 0:
            return "REFUND"
        elif issue.discount == 0 and issue.refund == 0 and issue.additional_credit > 0:
            return "CREDIT"
        elif (issue.discount > 0 or issue.refund > 0) and issue.additional_credit > 0:
            return "MIXED"
        elif issue.concession_total == 0:
            return "NO_COMPENSATION"

    return "UNKNOWN"


def reason_category(reason_number: str) -> str:
    reason_map = {
        # ARRIVED_TOO_LATE
        "20000001000": "ARRIVED_TOO_LATE",  # late order - past delivery day
        "20000002000": "ARRIVED_TOO_LATE",  # late order - past delivery window

        # LOW_QUALITY
        "20006000000": "LOW_QUALITY",  # Delivery Service
        "20004000000": "LOW_QUALITY",  # Driver Behavior
        "20012001000": "LOW_QUALITY",  # Food Safety - Illness - Food Born
        "21000001000": "LOW_QUALITY",  # Food Safety - Illness - Food Born
        "20012002000": "LOW_QUALITY",  # Food Safety - Illness - Allergy
        "21000002000": "LOW_QUALITY",  # Food Safety - Illness - Allergy
        "20012003000": "LOW_QUALITY",  # Food Safety - Illness - Injury
        "21000003000": "LOW_QUALITY",  # Food Safety - Illness - Injury
        "20012004000": "LOW_QUALITY",  # Food Safety - Illness - Other
        "21000004000": "LOW_QUALITY",  # Food Safety - Illness - Other
        "20012005000": "LOW_QUALITY",  # Food Safety - Insect - Farm
        "21000005000": "LOW_QUALITY",  # Food Safety - Insect - Farm
        "20012006000": "LOW_QUALITY",  # Food Safety - Insect - Non-farm
        "21000006000": "LOW_QUALITY",  # Food Safety - Insect - Non-farm
        "20012007000": "LOW_QUALITY",  # Food Safety - Foreign Object - Metal
        "21000007000": "LOW_QUALITY",  # Food Safety - Foreign Object - Metal
        "20012008000": "LOW_QUALITY",  # Food Safety - Foreign Object - Wood
        "21000008000": "LOW_QUALITY",  # Food Safety - Foreign Object - Wood
        "20012009000": "LOW_QUALITY",  # Food Safety - Foreign Object - Hair
        "21000009000": "LOW_QUALITY",  # Food Safety - Foreign Object - Hair
        "20012010000": "LOW_QUALITY",  # Food Safety - Foreign Object - Paper
        "21000010000": "LOW_QUALITY",  # Food Safety - Foreign Object - Paper
        "20012011000": "LOW_QUALITY",  # Food Safety - Foreign Object - Plastic
        "21000011000": "LOW_QUALITY",  # Food Safety - Foreign Object - Plastic
        "20012012000": "LOW_QUALITY",  # Food Safety - Foreign Object - Other
        "21000012000": "LOW_QUALITY",  # Food Safety - Foreign Object - Other
        "21002001000": "LOW_QUALITY",  # Quality - Discolored
        "21002002000": "LOW_QUALITY",  # Quality - Dry
        "21002003000": "LOW_QUALITY",  # Quality - Moldy
        "21002004000": "LOW_QUALITY",  # Quality - Overripe/Rotten
        "21002005000": "LOW_QUALITY",  # Quality - Shriveled/Wilted
        "21002006000": "LOW_QUALITY",  # Quality - Smelly
        "21002007000": "LOW_QUALITY",  # Quality - Underripe
        "21002008000": "LOW_QUALITY",  # Quality - Vendor Expiration Date Passed
        "21002009000": "LOW_QUALITY",  # Quality - Wet
        "21002010000": "LOW_QUALITY",  # Quality - Fatty
        "21002011000": "LOW_QUALITY",  # Quality - Dirty
        "21002012000": "LOW_QUALITY",  # Quality - Scarred
        "21002013000": "LOW_QUALITY",  # Quality - Naturally Occuring Object

        # NOT_AS_DESCRIBED
        "21004001000": "NOT_AS_DESCRIBED",  # Mismeasured Ingredient - Under Spec
        "21004002000": "NOT_AS_DESCRIBED",  # Mismeasured Ingredient - Over Spec
        "21004003000": "NOT_AS_DESCRIBED",  # Mismeasured Ingredient - Uneven Sizes
        "21001001000": "NOT_AS_DESCRIBED",  # Preference - Portion
        "21001002000": "NOT_AS_DESCRIBED",  # Preference - Taste
        "21001003000": "NOT_AS_DESCRIBED",  # Preference - Value
        "21001004000": "NOT_AS_DESCRIBED",  # Preference - Packaging
        "21001005000": "NOT_AS_DESCRIBED",  # Preference - Presentation
        "21001006000": "NOT_AS_DESCRIBED",  # Preference - Not Specified

        # ITEM_NOT_RECEIVED
        "21008000000": "ITEM_NOT_RECEIVED",  # Missing Knick Knack Bag
        "21005001000": "ITEM_NOT_RECEIVED",  # Missing Ingredient(s) - Missing
        "21005002000": "ITEM_NOT_RECEIVED",  # Missing Ingredient(s) - Empty Package
        "21009000000": "ITEM_NOT_RECEIVED",  # Missing Item
        "21012000000": "ITEM_NOT_RECEIVED",  # Missing Nutritional Label
        "210013000000": "ITEM_NOT_RECEIVED",  # Missing Recipe Card

        # ENTIRE_ORDER_NOT_RECEIVED
        "20001001000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Missed Commitment
        "20001002000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Carrier Damaged/Discarded
        "20001003000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Lost In Transit
        "20001004000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Marked Delivered (But No Box)
        "20001005000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Marked Delivered (Wrong Address - Carrier)
        "20001006000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Marked Delivered (Wrong Address - Customer)
        "20001007000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Customer Unavailable
        "20001009000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Still In Transit
        "20001010000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Weather
        "20001008000": "ENTIRE_ORDER_NOT_RECEIVED",  # Order Not Delivered - Not Tendered

        # DAMAGED_GOODS
        "20003000000": "DAMAGED_GOODS",  # Appeared Tampered With
        "20009000000": "DAMAGED_GOODS",  # Damaged Ice Packs
        "20008000000": "DAMAGED_GOODS",  # Delivered Damaged to Customer
        "20011000000": "DAMAGED_GOODS",  # Warm Box
        "20010000000": "DAMAGED_GOODS",  # Frozen Box
        "21003001000": "DAMAGED_GOODS",  # Damaged - Ingredient Damage
        "21003002000": "DAMAGED_GOODS",  # Damaged - Cold Damage
        "21003003000": "DAMAGED_GOODS",  # Damaged - Leaked in Bag
        "21003004000": "DAMAGED_GOODS",  # Damaged - Leaked in Box
        "21003005000": "DAMAGED_GOODS",  # Damaged - Packaging Failure
        "21003006000": "DAMAGED_GOODS",  # Damaged - Soggy
        "21006000000": "DAMAGED_GOODS",  # Damaged Knick Knack Bag

        # OTHER
        "20002002000": "OTHER",  # delivery address / instructions - instructions not followed
        "20002001000": "OTHER",  # delivery address / instructions - Change Address / Instructions
        "20005000000": "OTHER",  # early order
        "20013000000": "OTHER",  # Payment Issue
        "20007000000": "OTHER",  # Unexpected Box
        "21007000000": "OTHER",  # Extra Knick Knack Bag
        "21010000000": "OTHER",  # Short
        "21011000000": "OTHER",  # Swap
    }

    return reason_map.get(reason_number, "UNKNOWN")


def write_refund_csv(refund_lines: List[Dict[str, Any]], date):
    if not refund_lines:
        return

    columns = [
        "orderId", "eventTime", "eventId",
        "compensationStatus.itemStatus[].basicItemData.productId",
        "compensationStatus.itemStatus[].basicItemData.name",
        "compensationStatus.itemStatus[].basicItemData.quantity",
        "compensationStatus.itemStatus[].basicItemData.category",
        "compensationStatus.itemStatus[].basicItemData.type",
        "compensationStatus.itemStatus[].basicItemData.price.amountUSD",
        "compensationStatus.itemStatus[].basicItemData.price.amountLocalCurrency",
        "compensationStatus.itemStatus[].basicItemData.price.currency",
        "compensationStatus.itemStatus[].statusData.updatedStatus",
        "compensationStatus.itemStatus[].statusData.compensationTypeGranted",
        "compensationStatus.itemStatus[].statusData.reasonCategory",
        "compensationStatus.itemStatus[].statusData.internalReasonCategory",
        "compensationStatus.itemStatus[].statusData.returnMethodGranted",
        "compensationStatus.itemStatus[].statusData.returnCondition",
        "compensationStatus.itemStatus[].statusData.statusLog.shippedByCustomerTime",
        "compensationStatus.itemStatus[].statusData.statusLog.arrivedToWarehouseTime",
        "compensationStatus.totalGrantedAmount.amountUSD",
        "compensationStatus.totalGrantedAmount.amountLocalCurrency",
        "compensationStatus.totalGrantedAmount.currency",
        "compensationStatus.replacementOrderId",
        "compensationStatus.shippingRefundedAmount.amountUSD",
        "compensationStatus.shippingRefundedAmount.amountLocalCurrency",
        "compensationStatus.shippingRefundedAmount.currency",
        "compensationStatus.hasProofOfPurchase"
    ]

    df = pd.DataFrame(refund_lines, columns=columns)
    filename = f"refunds-{date.strftime('%Y-%m-%d')}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False, encoding='utf-8')
    logging.info(f"退款数据已写入: {filename}")


def main():
    logging.info("开始数据导出任务...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    start = datetime(2025, 8, 11)
    end = datetime(2025, 9, 30)

    mysql.connector.connect(**db_config)
    order_refund_history_for_forter(start, end)
    logging.info("数据导出完成")

    totalTime.sleep(36000)


if __name__ == "__main__":
    main()
