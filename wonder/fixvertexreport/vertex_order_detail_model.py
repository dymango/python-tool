from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import List, Optional


class ItemSubtype(Enum):
    ALCOHOLIC_BEVERAGE = "ALCOHOLIC_BEVERAGE"
    NON_ALCOHOLIC_BEVERAGE = "NON_ALCOHOLIC_BEVERAGE"
    FOOD = "FOOD"


class BlueApronProductType(Enum):
    STANDARD_MEAL_KIT = "STANDARD_MEAL_KIT"
    PREMIUM_MEAL_KIT = "PREMIUM_MEAL_KIT"
    PREMIUM_VARIANT_OF_STANDARD_MEAL_KIT = "PREMIUM_VARIANT_OF_STANDARD_MEAL_KIT"
    MINI_MEAL_KIT = "MINI_MEAL_KIT"
    OCCASION_BASED_OFFERING_MEAL_KIT = "OCCASION_BASED_OFFERING_MEAL_KIT"
    FIFTEEN_MINUTE_MEAL_KIT = "FIFTEEN_MINUTE_MEAL_KIT"
    STANDARD_PREPARED_AND_READY = "STANDARD_PREPARED_AND_READY"
    FAMILY_STYLE_PREPARED_AND_READY = "FAMILY_STYLE_PREPARED_AND_READY"
    EXTRA_PROTEINS_ADD_ON = "EXTRA_PROTEINS_ADD_ON"
    ASSEMBLE_AND_BAKE = "ASSEMBLE_AND_BAKE"


# 枚举定义
class BrandCategory(Enum):
    WONDER_MARKET = "WONDER_MARKET"
    ENVOY = "ENVOY"
    WONDER_MRC = "WONDER_MRC"
    WONDER_HDR = "WONDER_HDR"
    WONDER_LOCAL = "WONDER_LOCAL"
    BLUE_APRON = "BLUE_APRON"

    def hdr(self) -> bool:
        return self in [BrandCategory.WONDER_HDR, BrandCategory.WONDER_MRC]

    def blue_apron(self) -> bool:
        return self == BrandCategory.BLUE_APRON


class OrderChannel(Enum):
    APP = "APP"
    WEB = "WEB"
    IN_PERSON = "IN_PERSON"
    UBER_EATS = "UBER_EATS"
    SEAMLESS = "SEAMLESS"
    GRUB_HUB = "GRUB_HUB"
    DOOR_DASH = "DOOR_DASH"
    CAVIAR = "CAVIAR"
    POSTMATES = "POSTMATES"
    CCP = "CCP"
    BA_APP = "BA_APP"
    BA_WEB = "BA_WEB"
    BA_LEGACY = "BA_LEGACY"

    def third_party(self) -> bool:
        third_party_channels = [
            OrderChannel.UBER_EATS, OrderChannel.SEAMLESS, OrderChannel.GRUB_HUB,
            OrderChannel.DOOR_DASH, OrderChannel.CAVIAR, OrderChannel.POSTMATES
        ]
        return self in third_party_channels


class ScheduleType(Enum):
    ON_DEMAND = "ON_DEMAND"
    SCHEDULED = "SCHEDULED"
    ONE_TIME_PURCHASE = "ONE_TIME_PURCHASE"
    SUBSCRIPTION = "SUBSCRIPTION"


class DiningOption(Enum):
    DELIVERY = "DELIVERY"
    PICKUP = "PICKUP"


class OrderLogicType(Enum):
    WONDER_HDR_1P = "WONDER_HDR_1P"
    WONDER_SPOT = "WONDER_SPOT"
    WONDER_HDR_3P = "WONDER_HDR_3P"
    WONDER_HDR_3P_CORPORATE = "WONDER_HDR_3P_CORPORATE"
    LOCAL_STREAM = "LOCAL_STREAM"
    LOCAL_GRUBHUB = "LOCAL_GRUBHUB"
    BLUE_APRON = "BLUE_APRON"
    REMAKE = "REMAKE"
    MBB = "MBB"
    WONDER_MRC = "WONDER_MRC"
    BA_LEGACY = "BA_LEGACY"


class OrderStatus(Enum):
    PENDING_PAYMENT = "PENDING_PAYMENT"
    PAID = "PAID"
    PACKING = "PACKING"
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    IN_TRANSIT = "IN_TRANSIT"
    ARRIVED = "ARRIVED"
    IN_COOKING = "IN_COOKING"
    FOOD_IS_READY = "FOOD_IS_READY"
    DELIVERED = "DELIVERED"
    CANCELED = "CANCELED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    PICKUP_COMPLETE = "PICKUP_COMPLETE"
    DELIVERING = "DELIVERING"
    COMPLETE = "COMPLETE"

    def completed(self) -> bool:
        return self in [OrderStatus.DELIVERED, OrderStatus.COMPLETE]

    def in_progress(self) -> bool:
        in_progress_statuses = [
            OrderStatus.PAID, OrderStatus.PENDING, OrderStatus.PACKING,
            OrderStatus.ASSIGNED, OrderStatus.IN_TRANSIT, OrderStatus.ARRIVED,
            OrderStatus.IN_COOKING, OrderStatus.FOOD_IS_READY, OrderStatus.READY_FOR_PICKUP,
            OrderStatus.PICKUP_COMPLETE, OrderStatus.DELIVERING
        ]
        return self in in_progress_statuses


# 数据类定义
@dataclass
class Order:
    id: str
    order_number: str
    user_id: str
    brand_category: BrandCategory
    order_channel: OrderChannel
    schedule_type: ScheduleType
    dining_option: DiningOption
    order_logic_type: OrderLogicType
    status: OrderStatus
    need_utensils: bool
    service_date: date
    order_date: datetime


@dataclass
class OrderItem:
    id: str
    initial_order_item_id: str
    order_id: str
    order_bundle_item_id: str
    restaurant_id: str
    item_number: str
    global_menu_item_id: str
    external_id: str
    external_order_item_id: str
    menu_item_id: str
    menu_item_uid: str
    menu_item_sub_name: str
    menu_item_subtitle: str
    menu_item_sub_count: int
    brand_menu_item_id: str
    menu_item_name: str
    menu_item_tax_category_id: str
    menu_item_category_id: str
    menu_item_category_name: str
    image_key: str
    featured_image_key: str
    original_base_price: Optional[float]
    base_price: float
    unit_price: float
    original_order_quantity: int
    order_quantity: int
    ship_quantity: int
    deleted: Optional[bool]
    item_subtype: ItemSubtype
    business_line: Optional[str]
    note: str
    base_serving_size: Optional[int]
    selected_serving_size: Optional[int]
    selected_quantity: Optional[int]
    display_additional_price: Optional[float]
    product_id: Optional[str]
    cycle_id: Optional[str]
    enable_select_serving_quantity: Optional[bool]
    blue_apron_product_type: Optional[BlueApronProductType]
    preset: Optional[bool]
    created_time: datetime
    created_by: str
    updated_time: datetime
    updated_by: str


@dataclass
class OrderAddress:
    state: str
    zip_code: str
    county: str
    city: str
    address_line: str

    def full_zip_code(self) -> str:
        return self.zip_code


@dataclass
class OrderLocation:
    state_code: str
    zip_code: str
    county: str
    city: str
    address_line1: str


@dataclass
class OrderHDRAddress:
    order_id: str
    hdr_name: str


@dataclass
class OrderCharge:
    order_id: str
    subtotal: float
    discount: float
    promotion: float
    membership_subtotal: float
    subscription_save_discount: float
    small_order_fee: Optional[float] = None
    service_fee: Optional[float] = None
    fast_pass_fee: Optional[float] = None
    delivery_fee: Optional[float] = None
    adjust_subtotal: float = 0.0


@dataclass
class OrderChargeItem:
    order_id: str
    order_item_id: str
    menu_item_refundable_subtotal: float = 0.0
    remaining_small_order_fee: float = 0.0
    remaining_service_fee: float = 0.0
    remaining_fast_pass_fee: float = 0.0
    remaining_delivery_fee: float = 0.0


@dataclass
class OrderRestaurant:
    order_id: str
    facility_code: Optional[str] = None


@dataclass
class OrderTaxableCharge:
    taxable_subtotal: float
    taxable_small_order_fee: float
    taxable_service_fee: float
    taxable_fast_pass_fee: float
    taxable_delivery_fee: float


@dataclass
class OrderTaxableChargeItem:
    item_id: str
    menu_item_tax_category_id: str
    bundle_id: str
    taxable_subtotal: float
    taxable_small_order_fee: float
    taxable_service_fee: float
    taxable_fast_pass_fee: float
    taxable_delivery_fee: float
