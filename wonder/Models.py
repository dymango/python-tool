# models.py
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass
class Order:
    id: str
    user_id: str
    order_channel: str
    dining_option: str
    created_time: datetime
    status: str
    remake_ref_order_id: Optional[str] = None


@dataclass
class Customer:
    user_id: str
    email: str
    phone: Optional[str]
    first_name: str
    last_name: str
    created_time: datetime


@dataclass
class OrderItem:
    id: str
    menu_item_name: str
    order_quantity: int
    restaurant_id: str


@dataclass
class OrderChargeItem:
    order_item_id: str
    subtotal: Decimal
    adjust_subtotal: Decimal
    discount: Decimal
    promotion: Decimal
    membership_subtotal: Decimal
    subscription_save_discount: Decimal


@dataclass
class OrderCharge:
    final_amount: Decimal


@dataclass
class OrderRestaurant:
    restaurant_id: str
    restaurant_name: str


@dataclass
class OrderPayment:
    id: str
    payment_method: str
    credit_card_id: Optional[str] = None
    account_number: Optional[str] = None
    brand: Optional[str] = None
    revised_auth_amount: Decimal = Decimal(0)
    capture_amount: Optional[Decimal] = None
    refund_amount: Optional[Decimal] = None


@dataclass
class StripePaymentIntent:
    payment_id: str
    stripe_payment_method_id: str


@dataclass
class OrderAddress:
    address_line: str
    unit_number_or_company: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None


@dataclass
class OrderHDRAddress:
    address2: Optional[str] = None


@dataclass
class OrderLocation:
    address_line1: str
    city: str
    state_code: str
    zip_code: str


@dataclass
class OrderIssue:
    id: str
    order_id: str
    issue_type: str
    created_time: datetime
    discount: Decimal
    refund: Decimal
    additional_credit: Decimal
    concession_total: Decimal


@dataclass
class OrderIssueItem:
    issue_quantity: Optional[int]
    reason_number: str
    issue_order_id: str
    issue_order_item_id: Optional[str]
    issue_category: str


@dataclass
class OrderLine:
    order: 'Order'
    customer: 'Customer'
    order_item: 'OrderItem'
    order_charge_item: 'OrderChargeItem'
    order_charge: 'OrderCharge'
    order_restaurant: 'OrderRestaurant'
    order_payments: list  # List[OrderPayment]
    stripe_payment_intents: list  # List[StripePaymentIntent]
    order_address: 'OrderAddress'
    order_hdr_address: 'OrderHDRAddress'
    order_location: 'OrderLocation'
