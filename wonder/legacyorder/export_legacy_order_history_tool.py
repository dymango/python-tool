from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from enum import Enum
import logging
import math
import mysql.connector
from mysql.connector import Error
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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


@dataclass
class ShipFromView:
    state_code: Optional[str] = None
    zip_code: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    address_line: Optional[str] = None
    hdr_name: Optional[str] = None
    facility_code: Optional[str] = None


@dataclass
class OMSOrderEventMessageTaxDetail:
    order_number: str
    schedule_type: ScheduleType
    post_complete: bool
    service_date: date
    order_channel: OrderChannel
    need_utensils: bool
    ship_from: Optional[ShipFromView]
    state_code: str
    zip_code: str
    county: str
    city: str
    address_line: str
    order_taxable_charge: OrderTaxableCharge
    order_taxable_charge_items: List[OrderTaxableChargeItem]


# 数据库访问类
class OrderRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def get_or_else_throw(self, order_id: str) -> Order:
        """根据order_id获取订单，如果不存在则抛出异常"""
        query = """
        SELECT id, order_number, brand_category, order_channel, schedule_type, 
               dining_option, order_logic_type, status, need_utensils, service_date, order_date
        FROM orders WHERE id = %s
        """
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            raise ValueError(f"Order not found with id: {order_id}")

        return Order(
            id=result['id'],
            order_number=result['order_number'],
            brand_category=BrandCategory(result['brand_category']),
            order_channel=OrderChannel(result['order_channel']),
            schedule_type=ScheduleType(result['schedule_type']),
            dining_option=DiningOption(result['dining_option']),
            order_logic_type=OrderLogicType(result['order_logic_type']),
            status=OrderStatus(result['status']),
            need_utensils=bool(result['need_utensils']),
            service_date=result['service_date'],
            order_date=result['order_date']
        )


class OrderItemRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def select_by_order_id(self, order_id: str) -> List[OrderItem]:
        query = """
        SELECT 
            id, initial_order_item_id, order_id, order_bundle_item_id, restaurant_id,
            item_number, global_menu_item_id, external_id, external_order_item_id,
            menu_item_id, menu_item_uid, menu_item_sub_name, menu_item_subtitle,
            menu_item_sub_count, brand_menu_item_id, menu_item_name, menu_item_tax_category_id,
            menu_item_category_id, menu_item_category_name, image_key, featured_image_key,
            original_base_price, base_price, unit_price, original_order_quantity,
            order_quantity, ship_quantity, deleted, item_subtype, business_line, note,
            base_serving_size, selected_serving_size, selected_quantity, display_additional_price,
            product_id, cycle_id, enable_select_serving_quantity, blue_apron_product_type,
            preset, created_time, created_by, updated_time, updated_by
        FROM order_items 
        WHERE order_id = %s AND (deleted IS NULL OR deleted = FALSE)
        """
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        results = cursor.fetchall()
        cursor.close()

        order_items = []
        for row in results:
            try:
                # 处理枚举类型
                item_subtype = ItemSubtype(row['item_subtype']) if row['item_subtype'] else None
                blue_apron_product_type = BlueApronProductType(row['blue_apron_product_type']) if row[
                    'blue_apron_product_type'] else None

                order_item = OrderItem(
                    id=row['id'],
                    initial_order_item_id=row['initial_order_item_id'],
                    order_id=row['order_id'],
                    order_bundle_item_id=row['order_bundle_item_id'],
                    restaurant_id=row['restaurant_id'],
                    item_number=row['item_number'],
                    global_menu_item_id=row['global_menu_item_id'],
                    external_id=row['external_id'],
                    external_order_item_id=row['external_order_item_id'],
                    menu_item_id=row['menu_item_id'],
                    menu_item_uid=row['menu_item_uid'],
                    menu_item_sub_name=row['menu_item_sub_name'],
                    menu_item_subtitle=row['menu_item_subtitle'],
                    menu_item_sub_count=row['menu_item_sub_count'] or 0,
                    brand_menu_item_id=row['brand_menu_item_id'],
                    menu_item_name=row['menu_item_name'],
                    menu_item_tax_category_id=row['menu_item_tax_category_id'],
                    menu_item_category_id=row['menu_item_category_id'],
                    menu_item_category_name=row['menu_item_category_name'],
                    image_key=row['image_key'],
                    featured_image_key=row['featured_image_key'],
                    original_base_price=float(row['original_base_price']) if row[
                                                                                 'original_base_price'] is not None else None,
                    base_price=float(row['base_price'] or 0),
                    unit_price=float(row['unit_price'] or 0),
                    original_order_quantity=row['original_order_quantity'] or 0,
                    order_quantity=row['order_quantity'] or 0,
                    ship_quantity=row['ship_quantity'] or 0,
                    deleted=bool(row['deleted']) if row['deleted'] is not None else None,
                    item_subtype=item_subtype,
                    business_line=row['business_line'],
                    note=row['note'],
                    base_serving_size=row['base_serving_size'],
                    selected_serving_size=row['selected_serving_size'],
                    selected_quantity=row['selected_quantity'],
                    display_additional_price=float(row['display_additional_price']) if row[
                                                                                           'display_additional_price'] is not None else None,
                    product_id=row['product_id'],
                    cycle_id=row['cycle_id'],
                    enable_select_serving_quantity=bool(row['enable_select_serving_quantity']) if row[
                                                                                                      'enable_select_serving_quantity'] is not None else None,
                    blue_apron_product_type=blue_apron_product_type,
                    preset=bool(row['preset']) if row['preset'] is not None else None,
                    created_time=row['created_time'],
                    created_by=row['created_by'],
                    updated_time=row['updated_time'],
                    updated_by=row['updated_by']
                )
                order_items.append(order_item)
            except Exception as e:
                logger.error(f"Error parsing order item {row.get('id')}: {e}")
                continue

        return order_items

class OrderAddressRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def select_by_order_id_or_else_null(self, order_id: str) -> Optional[OrderAddress]:
        query = """
        SELECT state, zip_code, county, city, address_line
        FROM order_addresses WHERE order_id = %s
        """
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        return OrderAddress(
            state=result['state'],
            zip_code=result['zip_code'],
            county=result['county'],
            city=result['city'],
            address_line=result['address_line']
        )


class OrderHDRAddressRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def get_by_order_id_or_else_null(self, order_id: str) -> Optional[OrderHDRAddress]:
        query = "SELECT order_id, hdr_name FROM order_hdr_addresses WHERE order_id = %s"
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        return OrderHDRAddress(
            order_id=result['order_id'],
            hdr_name=result['hdr_name']
        )


class OrderLocationRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def get_by_order_id_or_else_null(self, order_id: str) -> Optional[OrderLocation]:
        query = """
        SELECT state_code, zip_code, county, city, address_line1
        FROM order_locations WHERE order_id = %s
        """
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        return OrderLocation(
            state_code=result['state_code'],
            zip_code=result['zip_code'],
            county=result['county'],
            city=result['city'],
            address_line1=result['address_line1']
        )


class OrderChargeRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def select_by_order_id(self, order_id: str) -> OrderCharge:
        query = """
        SELECT order_id, subtotal, discount, promotion, membership_subtotal, 
               subscription_save_discount, small_order_fee, service_fee, 
               fast_pass_fee, delivery_fee, adjust_subtotal
        FROM order_charges WHERE order_id = %s
        """
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            raise ValueError(f"Order charge not found for order_id: {order_id}")

        return OrderCharge(
            order_id=result['order_id'],
            subtotal=float(result['subtotal'] or 0),
            discount=float(result['discount'] or 0),
            promotion=float(result['promotion'] or 0),
            membership_subtotal=float(result['membership_subtotal'] or 0),
            subscription_save_discount=float(result['subscription_save_discount'] or 0),
            small_order_fee=float(result['small_order_fee']) if result['small_order_fee'] is not None else None,
            service_fee=float(result['service_fee']) if result['service_fee'] is not None else None,
            fast_pass_fee=float(result['fast_pass_fee']) if result['fast_pass_fee'] is not None else None,
            delivery_fee=float(result['delivery_fee']) if result['delivery_fee'] is not None else None,
            adjust_subtotal=float(result['adjust_subtotal'] or 0)
        )


class OrderChargeItemRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def select_by_order_id(self, order_id: str) -> List[OrderChargeItem]:
        query = """
        SELECT order_id, order_item_id, 
               (subtotal - adjust_subtotal - discount - promotion - membership_subtotal - subscription_save_discount) as menu_item_refundable_subtotal,
               (COALESCE(small_order_fee, 0) - COALESCE(adjust_small_order_fee, 0)) as remaining_small_order_fee,
               (COALESCE(service_fee, 0) - COALESCE(adjust_service_fee, 0)) as remaining_service_fee,
               (COALESCE(fast_pass_fee, 0) - COALESCE(adjust_fast_pass_fee, 0)) as remaining_fast_pass_fee,
               (COALESCE(delivery_fee, 0) - COALESCE(adjust_delivery_fee, 0)) as remaining_delivery_fee
        FROM order_charge_items WHERE order_id = %s
        """
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        results = cursor.fetchall()
        cursor.close()

        return [OrderChargeItem(
            order_id=row['order_id'],
            order_item_id=row['order_item_id'],
            menu_item_refundable_subtotal=float(row['menu_item_refundable_subtotal'] or 0),
            remaining_small_order_fee=float(row['remaining_small_order_fee'] or 0),
            remaining_service_fee=float(row['remaining_service_fee'] or 0),
            remaining_fast_pass_fee=float(row['remaining_fast_pass_fee'] or 0),
            remaining_delivery_fee=float(row['remaining_delivery_fee'] or 0)
        ) for row in results]


class OrderRestaurantRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def select_by_order_id(self, order_id: str) -> List[OrderRestaurant]:
        query = "SELECT order_id, facility_code FROM order_restaurants WHERE order_id = %s"
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (order_id,))
        results = cursor.fetchall()
        cursor.close()

        return [OrderRestaurant(
            order_id=row['order_id'],
            facility_code=row['facility_code']
        ) for row in results]


# 工具类
class BlueApronFacilityGetter:
    @staticmethod
    def get_facility_code(order_restaurants: List[OrderRestaurant]) -> Optional[str]:
        """从订单餐厅列表中获取第一个非空的facility_code"""
        for order_restaurant in order_restaurants:
            if order_restaurant.facility_code and order_restaurant.facility_code.strip():
                return order_restaurant.facility_code
        return None


# 工具函数
def get_orElse(value: Optional[float], default: float) -> float:
    return value if value is not None else default


def maths_round(value: float) -> float:
    return round(value, 2)


# 业务逻辑类
class OrderChargeHelper:
    @staticmethod
    def menu_item_refundable_subtotal(order_charge: OrderCharge) -> float:
        return maths_round(
            order_charge.subtotal -
            order_charge.adjust_subtotal -
            order_charge.discount -
            order_charge.promotion -
            order_charge.membership_subtotal -
            order_charge.subscription_save_discount
        )

    @staticmethod
    def remaining_small_order_fee(order_charge: OrderCharge) -> float:
        return maths_round(get_orElse(order_charge.small_order_fee, 0.0))

    @staticmethod
    def remaining_service_fee(order_charge: OrderCharge) -> float:
        return maths_round(get_orElse(order_charge.service_fee, 0.0))

    @staticmethod
    def remaining_fast_pass_fee(order_charge: OrderCharge) -> float:
        return maths_round(get_orElse(order_charge.fast_pass_fee, 0.0))

    @staticmethod
    def remaining_delivery_fee(order_charge: OrderCharge) -> float:
        return maths_round(get_orElse(order_charge.delivery_fee, 0.0))


class ShipFromBuilder:
    @staticmethod
    def build(facility_code: str) -> Optional[ShipFromView]:
        if not facility_code or not facility_code.strip():
            return None
        ship_from = ShipFromView()
        ship_from.facility_code = facility_code
        return ship_from

    @staticmethod
    def build_from_hdr(order_hdr_address: Optional[OrderHDRAddress], order_location: Optional[OrderLocation]) -> Optional[ShipFromView]:
        if order_hdr_address and order_location:
            ship_from = ShipFromView()
            ship_from.hdr_name = order_hdr_address.hdr_name
            ship_from.state_code = order_location.state_code
            ship_from.zip_code = order_location.zip_code
            ship_from.county = order_location.county
            ship_from.city = order_location.city
            ship_from.address_line = order_location.address_line1
            return ship_from
        return None


class OMSOrderEventMessageTaxDetailBuilder:
    @staticmethod
    def build(param: 'TaxDetailParam') -> Optional[OMSOrderEventMessageTaxDetail]:
        if OMSOrderEventMessageTaxDetailBuilder.skip(param.order):
            return None

        tax_detail = OMSOrderEventMessageTaxDetail(
            order_number=param.order.order_number,
            schedule_type=param.order.schedule_type,
            post_complete=OMSOrderEventMessageTaxDetailBuilder.post_complete(param),
            service_date=OMSOrderEventMessageTaxDetailBuilder.service_date(param),
            order_channel=param.order.order_channel,
            need_utensils=param.order.need_utensils,
            ship_from=OMSOrderEventMessageTaxDetailBuilder.get_ship_from(param),
            state_code="",
            zip_code="",
            county="",
            city="",
            address_line="",
            order_taxable_charge=None,
            order_taxable_charge_items=[]
        )

        # 设置地址信息
        if param.order.dining_option == DiningOption.PICKUP:
            if param.order_location:
                tax_detail.state_code = param.order_location.state_code
                tax_detail.zip_code = param.order_location.zip_code
                tax_detail.county = param.order_location.county
                tax_detail.city = param.order_location.city
                tax_detail.address_line = param.order_location.address_line1
        else:
            if param.order_address:
                tax_detail.state_code = param.order_address.state
                tax_detail.zip_code = param.order_address.full_zip_code()
                tax_detail.county = param.order_address.county
                tax_detail.city = param.order_address.city
                tax_detail.address_line = param.order_address.address_line

        tax_detail.order_taxable_charge = OMSOrderEventMessageTaxDetailBuilder.order_taxable_charge(param.order_charge)
        tax_detail.order_taxable_charge_items = OMSOrderEventMessageTaxDetailBuilder.order_taxable_charge_items(
            param.order_items, param.order_charge_items
        )

        return tax_detail

    @staticmethod
    def skip(order: Order) -> bool:
        if order.order_logic_type == OrderLogicType.BA_LEGACY:
            return True
        return order.order_channel.third_party() or (not order.brand_category.hdr() and not order.brand_category.blue_apron())

    @staticmethod
    def get_ship_from(param: 'TaxDetailParam') -> Optional[ShipFromView]:
        if param.order.brand_category == BrandCategory.BLUE_APRON:
            return ShipFromBuilder.build(param.facility_code)
        return ShipFromBuilder.build_from_hdr(param.order_hdr_address, param.order_location)

    @staticmethod
    def service_date(param: 'TaxDetailParam') -> date:
        if param.order.brand_category.hdr():
            return param.order.service_date
        else:
            return param.order.order_date.date() if isinstance(param.order.order_date, datetime) else param.order.order_date

    @staticmethod
    def post_complete(param: 'TaxDetailParam') -> bool:
        if param.order.brand_category.hdr():
            return param.order.status.completed()
        else:
            return True

    @staticmethod
    def order_taxable_charge_items(order_items: List[OrderItem], order_charge_items: List[OrderChargeItem]) -> List[
        OrderTaxableChargeItem]:
        # 使用initial_order_item_id作为映射键，因为Java代码中使用了item.id作为键
        order_item_map = {item.id: item for item in order_items}
        result = []

        for charge_item in order_charge_items:
            # 根据charge_item的order_item_id查找对应的order_item
            order_item = order_item_map.get(charge_item.order_item_id)
            if not order_item:
                logger.warning(f"Order item not found for charge item: {charge_item.order_item_id}")
                continue

            taxable_item = OrderTaxableChargeItem(
                item_id=charge_item.order_item_id,
                menu_item_tax_category_id=order_item.menu_item_tax_category_id,
                bundle_id=order_item.order_bundle_item_id,
                taxable_subtotal=charge_item.menu_item_refundable_subtotal,
                taxable_small_order_fee=charge_item.remaining_small_order_fee,
                taxable_service_fee=charge_item.remaining_service_fee,
                taxable_fast_pass_fee=charge_item.remaining_fast_pass_fee,
                taxable_delivery_fee=charge_item.remaining_delivery_fee
            )
            result.append(taxable_item)

        return result

    @staticmethod
    def order_taxable_charge(order_charge: OrderCharge) -> OrderTaxableCharge:
        return OrderTaxableCharge(
            taxable_subtotal=OrderChargeHelper.menu_item_refundable_subtotal(order_charge),
            taxable_small_order_fee=OrderChargeHelper.remaining_small_order_fee(order_charge),
            taxable_service_fee=OrderChargeHelper.remaining_service_fee(order_charge),
            taxable_fast_pass_fee=OrderChargeHelper.remaining_fast_pass_fee(order_charge),
            taxable_delivery_fee=OrderChargeHelper.remaining_delivery_fee(order_charge)
        )


@dataclass
class TaxDetailParam:
    order: Order
    order_items: List[OrderItem]
    order_address: Optional[OrderAddress]
    order_hdr_address: Optional[OrderHDRAddress]
    order_location: Optional[OrderLocation]
    order_charge: OrderCharge
    facility_code: Optional[str] = None
    order_charge_items: List[OrderChargeItem] = field(default_factory=list)


# 主服务类
class TaxDetailService:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.db_connection = None

    def __enter__(self):
        self.db_connection = mysql.connector.connect(**self.db_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()

    def get_tax_detail(self, order_id: str) -> Optional[OMSOrderEventMessageTaxDetail]:
        """根据order_id获取税务详情"""
        try:
            # 初始化Repository
            order_repo = OrderRepository(self.db_connection)
            order_item_repo = OrderItemRepository(self.db_connection)
            order_address_repo = OrderAddressRepository(self.db_connection)
            order_hdr_address_repo = OrderHDRAddressRepository(self.db_connection)
            order_location_repo = OrderLocationRepository(self.db_connection)
            order_charge_repo = OrderChargeRepository(self.db_connection)
            order_charge_item_repo = OrderChargeItemRepository(self.db_connection)
            order_restaurant_repo = OrderRestaurantRepository(self.db_connection)

            # 构建参数

            order = order_repo.get_or_else_throw(order_id)

            # 检查是否需要跳过
            if OMSOrderEventMessageTaxDetailBuilder.skip(order):
                logger.info(f"Skipping order {order_id} due to skip conditions")
                return None

            # 获取相关数据
            order_items = order_item_repo.select_by_order_id(order_id)
            order_address = order_address_repo.select_by_order_id_or_else_null(order_id)
            order_hdr_address = order_hdr_address_repo.get_by_order_id_or_else_null(order_id)
            order_location = order_location_repo.get_by_order_id_or_else_null(order_id)
            order_charge = order_charge_repo.select_by_order_id(order_id)
            order_charge_items = order_charge_item_repo.select_by_order_id(order_id)

            # 获取facility code
            order_restaurants = order_restaurant_repo.select_by_order_id(order_id)
            facility_code = BlueApronFacilityGetter.get_facility_code(order_restaurants)

            param = TaxDetailParam(order, order_items, order_address, order_hdr_address, order_location, order_charge, facility_code, order_charge_items)
            # 构建税务详情
            return OMSOrderEventMessageTaxDetailBuilder.build(param)

        except Exception as e:
            logger.error(f"Build tax detail message failed for order {order_id}", exc_info=True)
            return None


# 使用示例
def main():
    # 数据库配置 - 添加了database参数
    db_config = {
        'host': 'ftiuat-flexible-consumer-db.mysql.database.azure.com',
        'database': 'order',
        'user': 'datadog',
        'password': 'jPT8Q#gL9XLo%6ls',
    }

    order_id = "10724a02-6f95-4612-96f6-00c095c16561"

    with TaxDetailService(db_config) as service:
        tax_detail = service.get_tax_detail(order_id)
        if tax_detail:
            logger.info(f"Successfully built tax detail for order {order_id}")
            tax_detail_dict = tax_detail.__dict__.copy()

            # 简单的JSON序列化
            def default_serializer(obj):
                if isinstance(obj, (date, datetime)):
                    return obj.isoformat()
                elif isinstance(obj, Enum):
                    return obj.value
                return str(obj)

            json_str = json.dumps(tax_detail_dict, default=default_serializer, indent=2, ensure_ascii=False)
            logger.info(json_str)
            # 这里可以继续处理tax_detail，比如发送到Vertex API
        else:
            logger.info(f"Failed to build tax detail for order {order_id}")


if __name__ == "__main__":
    main()