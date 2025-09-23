from datetime import datetime, time

import mysql.connector
import pandas as pd
from mysql.connector import errorcode

from Models import (
    Order, Customer, OrderItem, OrderChargeItem, OrderCharge,
    OrderRestaurant, OrderPayment, StripePaymentIntent,
    OrderAddress, OrderHDRAddress, OrderLocation, OrderLine
)

# Obtain connection string information from the portal

config = {
    'host': 'rfprodv2-flexible-wonder-db-replica-v4.mysql.database.azure.com',
    'user': 'xxx',
    'password': 'xxx'
}


# Construct connection string
def export():
    try:
        conn = mysql.connector.connect(**config)
        print("Connection established")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = conn.cursor(dictionary=True)
        date = datetime(2025, 9, 23)
        start_of_day = datetime.combine(date.date(), time.min)  # 00:00:00
        end_of_day = datetime.combine(date.date(), time.max)
        searchOrderSql = """
        SELECT id, user_id, order_channel, dining_option, created_time, status, remake_ref_order_id
            FROM `order`.orders
            WHERE id='0ac3a3a0-6b93-4724-8f5d-4312f9ed5763'
            LIMIT 1
        """
        # Read data
        cursor.execute(searchOrderSql)
        rows = cursor.fetchall()
        print("Read", cursor.rowcount, "row(s) of data.")

        # Print all rows
        for row in rows:
            print(row.__repr__())
            lines = order_lines(Order(**row), cursor)
            for l in lines:
                print(l.__repr__())
            export_to_excel(lines, "test.csv")
            # Cleanup
            conn.commit()
            print("Done.")

    finally:
        cursor.close()
        conn.close()


def order_lines(order: Order, cursor):
    # customer
    cursor.execute("""
        SELECT user_id, email, phone, first_name, last_name, created_time
        FROM customer.customers
        WHERE user_id=%s
    """, (order.user_id,))
    customer_data = cursor.fetchone()
    customer = Customer(**customer_data)

    print("customer done")

    # order_items
    cursor.execute("""
        SELECT id, menu_item_name, order_quantity, restaurant_id
        FROM order.order_items
        WHERE order_id=%s AND NOT deleted
    """, (order.id,))
    order_items = [OrderItem(**row) for row in cursor.fetchall()]
    print("order_items done")
    # order_charge_items
    cursor.execute("""
        SELECT order_item_id, subtotal, adjust_subtotal, discount, promotion, membership_subtotal, subscription_save_discount
        FROM order.order_charge_items
        WHERE order_id=%s
    """, (order.id,))
    order_charge_items = [OrderChargeItem(**row) for row in cursor.fetchall()]
    print("order_charge_items done")
    # order_charge
    cursor.execute("""
        SELECT final_amount
        FROM order.order_charges
        WHERE order_id=%s
    """, (order.id,))
    order_charge = OrderCharge(**cursor.fetchone())
    print("order_charge done")
    # order_restaurants
    cursor.execute("""
        SELECT restaurant_id, restaurant_name
        FROM order.order_restaurants
        WHERE order_id=%s
    """, (order.id,))
    order_restaurants = [OrderRestaurant(**row) for row in cursor.fetchall()]
    print("order_restaurants done")
    # order_payments
    cursor.execute("""
        SELECT id, payment_method, credit_card_id, account_number, brand, revised_auth_amount, capture_amount, refund_amount
        FROM order.order_payments
        WHERE order_id=%s
    """, (order.id,))
    order_payments = [OrderPayment(**row) for row in cursor.fetchall()]
    print("order_payments done")
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
    print("order_address done")
    # order_hdr_address
    cursor.execute("""
        SELECT address2
        FROM order.order_hdr_addresses
        WHERE order_id=%s
    """, (order.id,))
    order_hdr_address = OrderHDRAddress(**cursor.fetchone())
    print("order_hdr_address done")
    # order_location
    cursor.execute("""
        SELECT address_line1, city, state_code, zip_code
        FROM order.order_locations
        WHERE order_id=%s
    """, (order.id,))
    order_location = OrderLocation(**cursor.fetchone())
    print("order_location done")
    # 构造 order_lines
    result = []
    for item in order_items:
        charge_item = next(oci for oci in order_charge_items if oci.order_item_id == item.id)
        restaurant = next(r for r in order_restaurants if r.restaurant_id == item.restaurant_id)
        line = OrderLine(
            order=order,
            customer=customer,
            order_item=item,
            order_charge_item=charge_item,
            order_charge=order_charge,
            order_restaurant=restaurant,
            order_payments=order_payments,
            stripe_payment_intents=stripe_payment_intents,
            order_address=order_address,
            order_hdr_address=order_hdr_address,
            order_location=order_location
        )
        result.append(line)

    return result


def order_line_to_dict(order_line):
    """按照Scala代码的映射逻辑将OrderLine对象转换为字典"""

    def safe_get(attr_path, default=""):
        """安全获取嵌套属性"""
        try:
            value = order_line
            for attr in attr_path.split('.'):
                if value is None:
                    return default
                value = getattr(value, attr, None)
            return value if value is not None else default
        except (AttributeError, IndexError, TypeError):
            return default

    def refundable_subtotal(charge_item):
        """计算可退款金额（模拟Scala中的refundableSubtotal方法）"""
        if not charge_item:
            return ""
        # 这里需要根据实际业务逻辑实现，暂时返回subtotal
        return str(charge_item.subtotal) if charge_item.subtotal else ""

    # 查找支付方式
    def find_payment(method):
        for payment in (order_line.order_payments or []):
            if payment.payment_method == method:
                return payment
        return None

    # 查找Stripe支付token
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

    # 地址处理逻辑（按照Scala代码的优先级）
    address_line1 = safe_get('order_address.address_line') or safe_get('order_location.address_line1')
    address_line2 = safe_get('order_address.unit_number_or_company') or safe_get('order_hdr_address.address2')
    city = safe_get('order_address.city') or safe_get('order_location.city')
    state = safe_get('order_address.state') or safe_get('order_location.state_code')
    zip_code = safe_get('order_address.zip_code') or safe_get('order_location.zip_code')

    # 按照Scala代码的顺序构建字典
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

        "cartItems[].itemSpecificData.food.restaurantAddress.address1": safe_get('order_location.address_line1'),
        "cartItems[].itemSpecificData.food.restaurantAddress.address2": safe_get('order_hdr_address.address2'),
        "cartItems[].itemSpecificData.food.restaurantAddress.city": safe_get('order_location.city'),
        "cartItems[].itemSpecificData.food.restaurantAddress.country": "US",
        "cartItems[].itemSpecificData.food.restaurantAddress.region": safe_get('order_location.state_code'),
        "cartItems[].itemSpecificData.food.restaurantAddress.zip": safe_get('order_location.zip_code'),
        "cartItems[].itemSpecificData.food.restaurantId": safe_get('order_restaurant.restaurant_id'),
        "cartItems[].itemSpecificData.food.restaurantName": safe_get('order_restaurant.restaurant_name'),

        "checkoutTime": str(int(safe_get('order.created_time').timestamp())) if safe_get('order.created_time') else "",
        "connectionInformation.customerIP": "127.0.0.1",
        "historicalData.fraud": "",
        "historicalData.orderStatus": "",
        "orderId": safe_get('order.id'),
        "orderType": "WEB" if safe_get('order.order_channel') == "WEB" else "UNKNOWN",

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
        "totalAmount.amountUSD": str(safe_get('order_charge.final_amount')) if safe_get(
            'order_charge.final_amount') else "",
        "totalAmount.currency": ""
    }

    return data


def export_to_excel(order_lines, filename=None):
    """将OrderLine列表导出为与CSV格式相同的Excel文件"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"forter-export-{timestamp}.xlsx"

    # 转换为字典列表
    data = [order_line_to_dict(line) for line in order_lines]
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
        'payment[].androidPay.lastFourDigits', 'payment[].androidPay.nameOnCard', 'payment[].applePay.bin',
        'payment[].applePay.expirationMonth', 'payment[].applePay.expirationYear',
        'payment[].applePay.lastFourDigits',
        'payment[].applePay.nameOnCard', 'payment[].billingDetails.address.address1',
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

    # 创建DataFrame，保持CSV的列顺序
    if data:
        # 使用CSV文件的列顺序

        df = pd.DataFrame(data, columns=csv_columns)
    else:
        # 如果没有数据，创建空DataFrame但保持列顺序
        df = pd.DataFrame(columns=csv_columns)

    # 保存为Excel文件
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"数据已导出到: {filename}")
    return filename


export()
