# order_exporter.py
import logging
import os
import sys
import time as totalTime
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time, timedelta

import mysql.connector
import pandas as pd
import pytz

from models import (
    Order, Customer, OrderItem, OrderChargeItem, OrderCharge,
    OrderPayment, StripePaymentIntent,
    OrderAddress, OrderLine, OrderFlag
)

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'pool_name': 'custom_connection_pool',
    'pool_size': 10
}

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True
)

output_dir = os.getenv('OUTPUT_DIR', '/app/export_results')


def export_single_day(current_date):
    logging.info(f"开始处理日期: {current_date.strftime('%Y-%m-%d')}")

    conn = None
    try:
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - 数据库连接已建立")
        conn = mysql.connector.connect(pool_name='custom_connection_pool')
        return process_single_day(conn, current_date)
    except mysql.connector.Error as err:
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - 数据库连接失败: {err}")
        return False
    except Exception as e:
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - export_single_day error: {e}")
        return False
    finally:
        if conn:
            conn.close()


def process_single_day(conn, current_date):
    s = totalTime.time()
    try:

        timezone = pytz.timezone('America/New_York')
        start_of_day_ny = timezone.localize(
            datetime.combine(current_date, time.min)
        )
        end_of_day_ny = timezone.localize(
            datetime.combine(current_date, time.max)
        )

        start_of_day_utc = start_of_day_ny.astimezone(pytz.UTC)
        end_of_day_utc = end_of_day_ny.astimezone(pytz.UTC)
        totalLines = []
        skip = 0
        part = 1

        while True:
            with conn.cursor(dictionary=True) as cursor:
                searchOrderSql = """
                                    SELECT id, user_id, order_channel, dining_option, order_date, created_time, status, remake_ref_order_id
                                        FROM `order`.orders
                                        WHERE created_time >= %s AND created_time <= %s
                                        AND brand_category = 'BLUE_APRON'
                                        AND order_channel = 'BA_LEGACY'
                                        AND status in ('CANCELED', 'COMPLETE')
                                        LIMIT 100 OFFSET %s
                                    """
                cursor.execute(searchOrderSql, (start_of_day_utc, end_of_day_utc, skip))
                rows = cursor.fetchall()
                if len(rows) == 0:
                    break

                for row in rows:
                    try:
                        lines = order_lines(Order(**row), cursor)
                        totalLines.extend(lines)
                    except Exception as e:
                        logging.exception("发生异常")

                skip += 100

                if len(totalLines) >= 80000:
                    filename = f"orders_{current_date.strftime('%Y-%m-%d')}_p{part}.csv"
                    filepath = os.path.join(output_dir, filename)
                    export_to_excel(totalLines, filepath)
                    logging.info(f"{current_date.strftime('%Y-%m-%d')}_p{part} - 导出完成: {len(totalLines)} 条记录")
                    totalLines.clear()
                    part += 1

        if totalLines:
            filename = f"orders_{current_date.strftime('%Y-%m-%d')}_p{part}.csv"
            filepath = os.path.join(output_dir, filename)
            export_to_excel(totalLines, filepath)
            logging.info(f"{current_date.strftime('%Y-%m-%d')} - 导出完成: {len(totalLines)} 条记录")
        else:
            logging.info(f"{current_date.strftime('%Y-%m-%d')} - 无数据")

        e = totalTime.time()
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - 处理完成, 耗时: {e - s: .2f} 秒")
        return True

    except Exception as e:
        logging.info(f"{current_date.strftime('%Y-%m-%d')} - 处理过程中发生错误: {e}")
        return False


def export_with_threadpool():
    start = totalTime.time()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    start_date = datetime(2025, 7, 1)
    end_date = datetime(2025, 9, 30)    

    dates_to_process = []
    current_date = start_date
    while current_date <= end_date:
        dates_to_process.append(current_date)
        current_date += timedelta(days=1)

    logging.info(f"开始处理 {len(dates_to_process)} 天的数据")

    max_workers = min(8, len(dates_to_process))
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


def order_lines(order: Order, cursor):
    # customer
    cursor.execute("""
        SELECT user_id, email, phone, first_name, last_name, created_time
        FROM customer.customers
        WHERE user_id=%s
    """, (order.user_id,))
    customer_data = cursor.fetchone()
    customer = Customer(**customer_data)

    # order_items
    cursor.execute("""
        SELECT id, menu_item_name, order_quantity, restaurant_id
        FROM order.order_items
        WHERE order_id=%s AND NOT deleted
    """, (order.id,))
    order_items = [OrderItem(**row) for row in cursor.fetchall()]
    # order_charge_items
    cursor.execute("""
        SELECT order_item_id, subtotal, adjust_subtotal, discount, promotion, membership_subtotal, subscription_save_discount
        FROM order.order_charge_items
        WHERE order_id=%s
    """, (order.id,))
    order_charge_items = [OrderChargeItem(**row) for row in cursor.fetchall()]
    # order_charge
    cursor.execute("""
        SELECT final_amount
        FROM order.order_charges
        WHERE order_id=%s
    """, (order.id,))
    order_charge = OrderCharge(**cursor.fetchone())

    # order_payments
    cursor.execute("""
        SELECT id, payment_method, credit_card_id, account_number, brand, revised_auth_amount, capture_amount, refund_amount
        FROM order.order_payments
        WHERE order_id=%s
    """, (order.id,))
    order_payments = [OrderPayment(**row) for row in cursor.fetchall()]
    # stripe_payment_intents
    psp_payment_ids = [p.id for p in order_payments if p.payment_method in ('APPLE_PAY', 'GOOGLE_PAY')]
    if psp_payment_ids:
        format_strings = ','.join(['%s'] * len(psp_payment_ids))
        cursor.execute(f"""
            SELECT payment_id, stripe_payment_method_id
            FROM payment.stripe_payment_intents
            WHERE payment_id IN ({format_strings})
        """, tuple(psp_payment_ids))
        stripe_payment_intents = [StripePaymentIntent(**row) for row in cursor.fetchall()]
    else:
        stripe_payment_intents = []

    # order_address
    cursor.execute("""
        SELECT address_line, unit_number_or_company, city, state, zip_code
        FROM order.order_addresses
        WHERE order_id=%s
    """, (order.id,))
    addr_row = cursor.fetchone()
    order_address = OrderAddress(**addr_row) if addr_row else None

    result = []

    order_flags = []
    if order.status == 'CANCELED':
        cursor.execute("""
                        SELECT order_id, action, created_by
                        FROM order.order_flags
                        WHERE order_id = %s AND action = 'BO_CANCEL'
                    """, (order.id,))

        flags_data = cursor.fetchall()
        order_flags = [OrderFlag(**row) for row in flags_data]

    for item in order_items:
        charge_item = []
        if order_charge_items:
            charge_item = next(oci for oci in order_charge_items if oci.order_item_id == item.id)

        line = OrderLine(
            order=order,
            customer=customer,
            order_item=item,
            order_charge_item=charge_item,
            order_charge=order_charge,
            order_payments=order_payments,
            stripe_payment_intents=stripe_payment_intents,
            order_address=order_address,
            order_flags=order_flags
        )
        result.append(line)

    return result


def order_line_to_dict(order_line):
    def safe_get(attr_path, default=""):
        try:
            value = order_line
            for attr in attr_path.split('.'):
                if value is None:
                    return default
                value = getattr(value, attr, None)
            return default if value is None else value
        except (AttributeError, IndexError, TypeError):
            return default

    def refundable_subtotal(charge_item):
        if not charge_item:
            return ""
        return str(charge_item.subtotal) if charge_item.subtotal is not None else ""

    def find_payment(method):
        for payment in (order_line.order_payments or []):
            if payment.payment_method == method:
                return payment
        return None

    def find_stripe_token(payment_id):
        for stripe_payment in (order_line.stripe_payment_intents or []):
            if stripe_payment.payment_id == payment_id:
                return stripe_payment.stripe_payment_method_id
        return ""

    google_pay = find_payment("GOOGLE_PAY")
    apple_pay = find_payment("APPLE_PAY")
    credit_card = find_payment("CREDIT_CARD")

    google_pay_token = find_stripe_token(google_pay.id) if google_pay else ""
    apple_pay_token = find_stripe_token(apple_pay.id) if apple_pay else ""

    address_line1 = safe_get('order_address.address_line') or ''
    address_line2 = safe_get('order_address.unit_number_or_company') or ''
    city = safe_get('order_address.city') or ''
    state = safe_get('order_address.state') or ''
    zip_code = safe_get('order_address.zip_code') or ''

    data = {
        "accountOwner.accountId": safe_get('customer.user_id'),
        "accountOwner.created": str(int(safe_get('customer.created_time').timestamp())) if safe_get(
            'customer.created_time') else "",
        "accountOwner.email": safe_get('customer.email'),
        "accountOwner.firstName": safe_get('customer.first_name'),
        "accountOwner.fullName": "",
        "accountOwner.lastName": safe_get('customer.last_name'),

        "cartItems[].basicItemData.name": safe_get('order_item.menu_item_name'),
        "cartItems[].basicItemData.quantity": str(safe_get('order_item.order_quantity')) if safe_get(
            'order_item.order_quantity') else "",
        "cartItems[].basicItemData.category": "",
        "cartItems[].basicItemData.price.amountLocalCurrency": "",
        "cartItems[].basicItemData.price.amountUSD": refundable_subtotal(order_line.order_charge_item),
        "cartItems[].basicItemData.price.currency": "",

        "cartItems[].beneficiaries[].personalDetails.email": "",
        "cartItems[].beneficiaries[].personalDetails.firstName": "",
        "cartItems[].beneficiaries[].personalDetails.fullName": "",
        "cartItems[].beneficiaries[].personalDetails.lastName": "",
        "cartItems[].beneficiaries[].phone[].phone": "",

        "cartItems[].itemSpecificData.food.restaurantAddress.address1": '',
        "cartItems[].itemSpecificData.food.restaurantAddress.address2": '',
        "cartItems[].itemSpecificData.food.restaurantAddress.city": '',
        "cartItems[].itemSpecificData.food.restaurantAddress.country": "US",
        "cartItems[].itemSpecificData.food.restaurantAddress.region": "",
        "cartItems[].itemSpecificData.food.restaurantAddress.zip": "",
        "cartItems[].itemSpecificData.food.restaurantId": "Blue Apron",
        "cartItems[].itemSpecificData.food.restaurantName": "Blue Apron",

        "checkoutTime": str(int(safe_get('order.order_date').timestamp())) if safe_get('order.order_date') else "",
        "connectionInformation.customerIP": '127.0.0.1',
        "historicalData.fraud": "",
        "historicalData.orderStatus": get_historical_order_status(order_line.order, order_line.order_flags),
        "orderId": safe_get('order.id'),
        "orderType": get_order_type(order_line.order),

        # Android Pay (Google Pay)
        "payment[].androidPay.bin": "",
        "payment[].androidPay.expirationMonth": "",
        "payment[].androidPay.expirationYear": "",
        "payment[].androidPay.lastFourDigits": "",
        "payment[].androidPay.nameOnCard": "",
        "payment[].androidPay.token": google_pay_token,

        # Apple Pay
        "payment[].applePay.bin": "",
        "payment[].applePay.expirationMonth": "",
        "payment[].applePay.expirationYear": "",
        "payment[].applePay.lastFourDigits": "",
        "payment[].applePay.nameOnCard": "",
        "payment[].applePay.token": apple_pay_token,

        # Billing Details
        "payment[].billingDetails.address.address1": "",
        "payment[].billingDetails.address.address2": "",
        "payment[].billingDetails.address.city": "",
        "payment[].billingDetails.address.country": "",
        "payment[].billingDetails.address.region": "",
        "payment[].billingDetails.address.zip": "",
        "payment[].billingDetails.personalDetails.email": "",
        "payment[].billingDetails.phone[].phone": "",
        "payment[].billingDetails.personalDetails.fullName": "",
        "payment[].billingDetails.personalDetails.firstName": "",
        "payment[].billingDetails.personalDetails.lastName": "",

        # Credit Card
        "payment[].creditCard.bin": "",
        "payment[].creditCard.expirationMonth": "",
        "payment[].creditCard.expirationYear": "",
        "payment[].creditCard.lastFourDigits": "",
        "payment[].creditCard.nameOnCard": "",
        "payment[].creditCard.verificationResults.processorResponseCode": "",
        "payment[].creditCard.verificationResults.processorResponseText": "",

        # Tokenized Card
        "payment[].tokenizedCard.bin": "",
        "payment[].tokenizedCard.expirationMonth": "",
        "payment[].tokenizedCard.expirationYear": "",
        "payment[].tokenizedCard.lastFourDigits": credit_card.account_number if credit_card else "",
        "payment[].tokenizedCard.verificationResults.processorResponseCode": "",
        "payment[].tokenizedCard.verificationResults.processorResponseText": "",
        "payment[].tokenizedCard.verificationResults.eciValue": "",
        "payment[].tokenizedCard.token": credit_card.credit_card_id if credit_card else "",

        # Delivery Details
        "primaryDeliveryDetails.deliveryMethod": safe_get('order.dining_option'),
        "primaryDeliveryDetails.deliveryType": "PHYSICAL",

        # Recipient Details
        "primaryRecipient.address.address1": address_line1,
        "primaryRecipient.address.address2": address_line2,
        "primaryRecipient.address.city": city,
        "primaryRecipient.address.zip": zip_code,
        "primaryRecipient.address.country": "US",
        "primaryRecipient.address.region": state,
        "primaryRecipient.personalDetails.email": safe_get('customer.email'),
        "primaryRecipient.phone[].phone": safe_get('customer.phone'),

        # Total Amount
        "totalAmount.amountLocalCurrency": "",
        "totalAmount.amountUSD": get_final_amount(order_line.order_charge),
        "totalAmount.currency": 'USD'
    }

    return data


def get_final_amount(orderCharge: OrderCharge):
    if orderCharge is None or orderCharge.final_amount is None:
        return "0"
    return str(orderCharge.final_amount)


def get_order_type(order: Order):
    if order.order_channel == 'BA_WEB' or order.order_channel == 'WEB':
        return 'WEB'
    elif order.order_channel == 'BA_APP' or order.order_channel == 'APP':
        return 'MOBILE'

    return "UNKNOWN"


def get_historical_order_status(order: Order, order_flags: list) -> str:
    if order.status == 'COMPLETE':
        return 'COMPLETE'

    elif order.status == 'CANCELED':
        for flag in order_flags:
            if flag.created_by == 'customer-service-site':
                return 'CANCELED_BY_MERCHANT'

        return 'CANCELED_BY_CUSTOMER'

    else:
        return ''


def export_to_excel(lines, filename=None):
    data = [order_line_to_dict(line) for line in lines]
    csv_columns = [
        'accountOwner.accountId', 'accountOwner.created', 'accountOwner.email',
        'accountOwner.firstName', 'accountOwner.fullName', 'accountOwner.lastName',
        'cartItems[].basicItemData.name', 'cartItems[].basicItemData.quantity',
        'cartItems[].basicItemData.category', 'cartItems[].basicItemData.price.amountLocalCurrency',
        'cartItems[].basicItemData.price.amountUSD', 'cartItems[].basicItemData.price.currency',
        'cartItems[].beneficiaries[].personalDetails.email',
        'cartItems[].beneficiaries[].personalDetails.firstName',
        'cartItems[].beneficiaries[].personalDetails.fullName',
        'cartItems[].beneficiaries[].personalDetails.lastName',
        'cartItems[].beneficiaries[].phone[].phone', 'cartItems[].itemSpecificData.food.restaurantAddress.address1',
        'cartItems[].itemSpecificData.food.restaurantAddress.address2',
        'cartItems[].itemSpecificData.food.restaurantAddress.city',
        'cartItems[].itemSpecificData.food.restaurantAddress.country',
        'cartItems[].itemSpecificData.food.restaurantAddress.region',
        'cartItems[].itemSpecificData.food.restaurantAddress.zip', 'cartItems[].itemSpecificData.food.restaurantId',
        'cartItems[].itemSpecificData.food.restaurantName', 'checkoutTime', 'connectionInformation.customerIP',
        'historicalData.fraud', 'historicalData.orderStatus', 'orderId', 'orderType',
        'payment[].androidPay.bin', 'payment[].androidPay.expirationMonth', 'payment[].androidPay.expirationYear',
        'payment[].androidPay.lastFourDigits', 'payment[].androidPay.nameOnCard', 'payment[].androidPay.token',
        'payment[].applePay.bin',
        'payment[].applePay.expirationMonth', 'payment[].applePay.expirationYear',
        'payment[].applePay.lastFourDigits',
        'payment[].applePay.nameOnCard', 'payment[].applePay.token', 'payment[].billingDetails.address.address1',
        'payment[].billingDetails.address.address2',
        'payment[].billingDetails.address.city', 'payment[].billingDetails.address.country',
        'payment[].billingDetails.address.region',
        'payment[].billingDetails.address.zip', 'payment[].billingDetails.personalDetails.email',
        'payment[].billingDetails.phone[].phone',
        'payment[].billingDetails.personalDetails.fullName', 'payment[].billingDetails.personalDetails.firstName',
        'payment[].billingDetails.personalDetails.lastName', 'payment[].creditCard.bin',
        'payment[].creditCard.expirationMonth',
        'payment[].creditCard.expirationYear', 'payment[].creditCard.lastFourDigits',
        'payment[].creditCard.nameOnCard',
        'payment[].creditCard.verificationResults.processorResponseCode',
        'payment[].creditCard.verificationResults.processorResponseText',
        'payment[].tokenizedCard.bin', 'payment[].tokenizedCard.expirationMonth',
        'payment[].tokenizedCard.expirationYear',
        'payment[].tokenizedCard.lastFourDigits',
        'payment[].tokenizedCard.verificationResults.processorResponseCode',
        'payment[].tokenizedCard.verificationResults.processorResponseText',
        'payment[].tokenizedCard.verificationResults.eciValue',
        'payment[].tokenizedCard.token', 'primaryDeliveryDetails.deliveryMethod',
        'primaryDeliveryDetails.deliveryType',
        'primaryRecipient.address.address1', 'primaryRecipient.address.address2', 'primaryRecipient.address.city',
        'primaryRecipient.address.zip', 'primaryRecipient.address.country', 'primaryRecipient.address.region',
        'primaryRecipient.personalDetails.email', 'primaryRecipient.phone[].phone',
        'totalAmount.amountLocalCurrency',
        'totalAmount.amountUSD', 'totalAmount.currency'
    ]

    if data:
        df = pd.DataFrame(data, columns=csv_columns)
    else:
        df = pd.DataFrame(columns=csv_columns)

    df.to_csv(filename, index=False, encoding='utf-8')
    return filename


def main():
    logging.info("开始数据导出任务...")
    mysql.connector.connect(**db_config)
    export_with_threadpool()
    logging.info("数据导出完成")
    totalTime.sleep(86400)


if __name__ == "__main__":
    main()
