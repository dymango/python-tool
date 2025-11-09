import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import List, Optional

import mysql.connector
from vertex_order_detail_model import *
import call_vertex_api
from model import *

db_config = {
    'host': 'rfprodv2-flexible-wonder-db-replica-v4.mysql.database.azure.com',
    'database': 'order',
    'user': 'datadog',
    'password': 'jPT8Q#gL9XLo%6ls',
}

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库访问类
class OrderRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def get_or_else_throw(self, order_id: str) -> Order:
        """根据order_id获取订单，如果不存在则抛出异常"""
        query = """
        SELECT id, user_id, order_number, brand_category, order_channel, schedule_type, 
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
            user_id=result['user_id'],
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
    def build_from_hdr(order_hdr_address: Optional[OrderHDRAddress], order_location: Optional[OrderLocation]) -> \
            Optional[ShipFromView]:
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
        return order.order_channel.third_party() or (
                not order.brand_category.hdr() and not order.brand_category.blue_apron())

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
            return param.order.order_date.date() if isinstance(param.order.order_date,
                                                               datetime) else param.order.order_date

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
    def __enter__(self):
        self.db_connection = mysql.connector.connect(**db_config)
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

            param = TaxDetailParam(order, order_items, order_address, order_hdr_address, order_location, order_charge,
                                   facility_code, order_charge_items)

            # 构建税务详情
            build = OMSOrderEventMessageTaxDetailBuilder.build(param)
            call_vertex_api.report_main(order_id, order.user_id, order.brand_category.name, build)

            return build
        except Exception as e:
            logger.error(f"Build tax detail message failed for order {order_id}", exc_info=True)
            return None


# 使用示例
def main():
    order_ids = [
        'e4cb11ab-3a2f-465c-a420-0e67cc6832aa',
        '7945b57e-c1ce-42d7-abab-a233883013ed',
        '14e161d8-a12f-4db9-aaa0-1c138adf66d2',
        '245ea5fe-29d9-41ea-8cc4-84b57299e7fb',
        'e27da516-34e3-4a82-b275-2b807876df50',
        '3a605055-417b-43d2-a202-f49b7ec39525',
        'eeebd46d-dd49-494f-bd83-cab4dfd6a082',
        '7fc67d4c-b360-4ef1-b858-3e7970017e7d',
        '4ad95612-28cb-45d8-97b3-06d8f7f96956',
        '3443bdcf-af74-4d04-a7d4-bd0f54aa5fa7',
        'cc732661-41ca-4b87-93b3-efa2991e0b5a',
        'f072ee60-4c37-4eae-9b6d-2e96b29b630f',
        '7d16c131-0d24-4491-9c86-3545707caf96',
        '4c3f05a0-dd59-48b7-ad1d-16cc9e460631',
        '35d87a50-94ad-4a0b-ba70-878c50ce2ef9',
        '88a390df-e0f3-4ede-a0d3-84897e1c853f',
        'fea0f515-04d4-49c8-bf1c-47723d360882',
        'eb71e0e9-ce62-4d56-a316-92678a6b7c7f',
        '5b3e7ac7-e186-47b0-b9e0-21bcb420fcf2',
        '92728f3d-fc87-4ff5-9a9b-ac6df6050db9',
        '78c5e279-e222-4c0c-b26f-460b3b6a6359',
        '322ba43b-6ec0-4164-a267-b5cf376b2bff',
        '986805cf-c5b2-4416-8a50-c2691e7d6fd1',
        '8314a751-8c0f-4608-ae0d-634ce667eb30',
        '50c0eb29-cbf0-44db-976a-c44b39ee07d3',
        '6b063b06-a39a-4d88-88a8-11f947fbbfd2',
        '3c0d6dc9-901f-4bda-82ab-e6163cbe91d6',
        '3f911119-3b3d-4a8e-9d6b-9b157b355eb8',
        '8c746108-c5fa-478f-9ff9-e1c6ac10ed4a',
        '1fa18844-dc68-4e24-86e5-7a90b9b6e126',
        'e95b0fc8-5610-41e0-ab38-81c7a33ee0ee',
        'adbfedde-7b7a-4c3b-95d3-40aa704db9a8',
        '56790f55-8841-4395-a5c7-27ecc7d73923',
        '6755a2c6-9545-42d8-8556-8269c0931f1c',
        'bd1b82e7-cc79-42a6-b8c1-6c63164578b5',
        '1b7b1944-b2d6-4561-adf7-d7e5d096999c',
        '3b660f88-2324-4e53-ba83-2da828d9082c',
        'e82c40ae-b736-4964-93a1-c27fbeea25a3',
        'fa45c371-8ec5-429a-a58c-afb81b1b8746',
        '03a365d9-beff-490c-a71e-b0b9ad480326',
        'bf08e939-8635-477d-9262-49e5906cae21',
        'f09dfb12-5948-471b-913a-5a5ec6b52c09',
        '1b451032-63de-4e6c-9011-ca054776315a',
        'a9e315f2-71c6-47d1-b43c-d279158d41a5',
        '3df5c88e-6bf4-49ae-99e7-39b39ee06d85',
        'd7dcab48-01e5-4c40-b12b-7f21162c84c1',
        'a9556a28-9ce4-499c-a706-aae6f095d1a8',
        '91d11a45-6a0d-4bb7-a47f-8226013a6275',
        '90b00393-1743-4031-8427-787ff94fa116',
        '7f24211f-c8ab-4cd3-a2ab-7e47c5e72e6c',
        'b27234d7-f894-4087-a26e-5f6b8c7d986d',
        '79291490-8c62-4e8b-9fa3-b9c07fab9fb4',
        'c2732499-7525-4829-9bcb-1eff9d645802',
        'e229bc1e-7c27-4842-b2b0-3811cef8b83c',
        'cc5e4aaa-53c1-4d45-9c65-477c95e275d9',
        'ace73906-e579-4b5f-918a-a572957d4e42',
        'e5b8f7c3-f8e0-47e8-afb3-d82847b42ee2',
        '4ea563f5-ca49-4e3e-9b98-fa11df15708d',
        '7a9e93d0-02e9-4b1b-9cb0-d999a2b09bbc',
        'c3893f8b-4440-4007-a230-12d110f1769b',
        'c5d3e2bd-715d-40ae-9625-4b70c7ed40cf',
        '1f2fda66-5e54-4e5c-8441-a1a92c3136f4',
        'ad9ed910-23a4-4254-98c2-aa75299b04df',
        '54603929-7b2e-4c4c-9609-8b094e823af1',
        '35538f33-ef04-4624-9cc7-47b3deca120e',
        '3c78fbd4-7f53-443d-b515-298f7b15bca3',
        'a48b04bf-ad32-4673-b1ac-a4feb363a724',
        '95d67a8c-1dd3-4674-a041-b000bb5b94f4',
        '2401a04b-4db9-4957-849f-985a50417fb3',
        'dc970fb4-7515-4888-941d-c40c92354c72',
        '03a200b7-8129-4ff5-b069-9e8160ca0102',
        'cdd666cb-9dab-429a-b854-99cbc7920309',
        '1ca356c5-8058-4588-9eaf-5cd3377bc9d3',
        '4d22fbd5-1204-42f7-9dda-7201d91c54f3',
        '47ea41e8-fde7-4fcf-bd98-8d2f37208145',
        '64937960-8890-4a1d-9060-3904733256f8',
        'bc9d5081-d910-4895-a1bb-114a4cb129e5',
        '0566d52d-d81c-4e9e-bd07-45b9b14674bb',
        'd250196f-8824-4716-87a8-9a614ce5a562',
        '91703477-bf26-4dae-a827-bd3acaa7693d',
        '7c3daf22-fc8c-48c8-878c-b5ac573527b4',
        'dd0529ed-cdcc-4511-a01c-eea7e386dbdd',
        '7865dd12-6f67-4475-b817-8ead76fe9586',
        'b88e5c18-3892-4876-97dd-4922f118db89',
        'c47e8e84-02dd-46bc-a3e6-7325cd5ef4c9',
        'dc9f72fe-24ac-4f54-9d96-1ef9699356e1',
        'c611c5cb-17d0-48a1-9931-2debac7a7c1f',
        '2abf2927-eac1-4666-9b6a-495aee62e219',
        '75208e01-8ef4-4209-b3c0-fbbdc41f1478',
        '50999562-fbbd-47d2-ba46-d7d36df34834',
        '72b4aebe-0c39-4d4f-a0d9-5a13ecb27f49',
        'fa9066b1-8573-4485-9334-739fb7129928',
        'cf9fee1e-8441-49b8-9775-4d074c08add2',
        '3a3cfcc0-a701-4600-a4cc-082c42ff9297',
        '440df920-d969-4ff7-b002-2191b9c33f41',
        '0466cec8-de52-4174-b6d1-4d3940305f9d',
        '062ac6b6-ceda-4801-b602-94ca877ba1fe',
        '6fb53460-53e6-463d-a7d2-e521bca97edd',
        '969f8054-0b54-4286-b070-e5a7fb4cb5e5',
        'f53fe72d-a22c-4618-9969-57e83f7db02d',
        'e27e6ddc-67d3-4440-84d2-54032d019a84',
        'ae7cd682-38fd-4ee1-962a-0620fb81060c',
        '611e7b2a-707e-42a3-ac7c-8db85b33321a',
        '027865af-9635-4be2-81e3-988813037c02',
        '224b066c-57ee-4665-92ad-60e504cbdd70',
        'a0ff7d44-f956-41a5-bf87-d1cc173cb057',
        '614ce725-59b6-4d0e-ac83-4916e95b5d78',
        'ea9b0961-e0bf-4ca3-8632-8b4994de05a9',
        '237c4b58-30a9-4bc1-88ae-1a1c935c3031',
        'c5b36db9-e093-431b-8b80-0771099966d4',
        '502658bf-8d93-4a28-b90e-9c124ccd45e7',
        'b56bb95f-78b7-4e09-a3ee-f87c53e3e184',
        '10538784-53b0-452f-9bc5-62caf48b123e',
        'a1603392-d71e-4fd2-8409-45628376d681',
        'e8ac8a22-636b-4f99-9693-14ea75ad557e',
        'b587a50f-95e3-4403-8bbe-ef00d96d4fc3',
        '3f0e8619-d73f-4727-84b1-df9a41e8ecaf',
        'c996e698-9510-48d6-907a-a6759169e2e6',
        'a94a291c-2b80-4bdf-ae15-eb0766c12ec0',
        '553534a4-bbd8-462a-834f-a6fe3de4ba94',
        '14057179-1b1f-4e6a-9adc-3f9d4608bdf6',
        '80a91ff5-b554-44dc-b0fe-1773c87ced2f',
        '927c84a7-8942-44f0-a455-76ea3cdc12cf',
        'a6785036-d1d3-44bc-9df7-bcd25b26cbeb',
        'a1789d6d-c660-46c7-86b7-719efc56397d',
        'c571738e-cdaa-4915-8243-3eba7b038d9e',
        'bda52a17-b999-46aa-a0d4-60c0b21da5dc',
        'ea2178a2-25bb-4bf6-80aa-9fc207dfc9e0',
        'ecc061ff-04b8-40d6-8735-abcc689b86fc',
        'c0c07f2b-17cf-4e9d-bcd6-0061812d9d4e',
        'a9089dc1-d673-4378-8d57-30ff922cfe68',
        '7b38e67f-8c69-4454-a12f-fe738e55fb19',
        'af409989-661b-4c47-9d41-b1d4bde99967',
        'ce7c1531-fcda-45ea-8b9a-3a12f135f970',
        '10a1b5cc-5503-4e96-929e-225ee1c254c5',
        '70cc8b01-88dd-4935-a25c-2e894dd5b5b1',
        '934d3f88-1246-483f-b505-c881283dff62',
        '4f1dee3e-8418-49e5-938f-3f387f157373',
        'caa03995-c488-46ed-a086-84bb0f423c29',
        'f6da3ac3-51cd-41f9-accd-d5c6ba681d3b',
        '21872b1f-2809-4d7f-852d-d10fcac5bb71',
        '4b67aa7a-e7e4-4425-ab98-49156ee20a14',
        '65549b1f-5ae1-4d28-b087-4ea0d782c520',
        'e8c424e2-7409-4c4b-8cbc-3457d917355a',
        '416d638f-0e8e-4a9e-80fd-f811e2c0280b',
        'a16f9acc-0b0c-49db-bf9e-fad0b9c769a8',
        'b2fcedf4-4182-4680-a89c-0cc6c6221b61',
        '214ce4c3-7a45-4c13-9df9-d841285c59c1',
        '64139843-9fe1-468c-8536-81faaf7b781d',
        'fefa6171-4c91-44d5-8d7b-d5425b09ae01',
        '795b2c74-579e-49ae-8c98-4515dbfaebbe',
        '83d70af2-f605-4c64-8253-3490201f6592',
        '7374b802-b28c-4cb9-8b78-43d2ec716771',
        'f470ddd6-b50a-4f28-b680-3d443a55ec56',
        '2b1bcf2e-6875-40cc-a1e4-78b431ad1a93',
        'f6a6fa7e-faa0-4676-86db-148342964e4b',
        '52381abd-cc71-465a-9d41-a8d2b209e963',
        '85b1de12-00a1-411c-9e9a-dc16ddeb14fa',
        '9c39fb02-1622-43c0-aa9e-3c5637c3c22d',
        '001fcdf7-66ad-448d-9536-5d1bb2138695',
        '201b689d-df96-4ff6-8df9-3e84e13c5c0c',
        'af525e01-cd99-4dbe-8376-8c5607c4003a',
        'bb9d7b38-d5b9-4c6e-a0bd-97d1dc6af3d6',
        '898e7fa0-fc81-4747-87be-585884584e9b',
        '77dcf2c0-d913-4107-b4ce-310b0922e639',
        'cf87a511-db10-4bfd-816f-e224cd32cc09',
        'dd45e6fa-5c48-440e-aa5b-ce5a16b4b69f',
        'e76dba65-d2ba-4e55-874d-4791d90f1c3c',
        'b514a36d-6199-49d0-9d81-b08225589193',
        '74364c58-fdef-411e-a8a3-ad8103289b45',
        'd54a7f4f-c9c3-47cd-b648-62065dfb9110',
        '604eaa37-8081-4789-95e0-6cd8f7591b47',
        'a8af7a33-2ae7-477c-b237-c42d979e6b3b',
        '6abedc08-f171-44db-8359-e8fbb8c7e72e',
        '04177889-3e03-49f4-91e2-fe5bccfd3c0b',
        '524441d3-05a1-4d06-bbff-47ce75406471',
        'be66ab2a-e0a2-4149-8cfb-e03ed88e0ce5',
        'f7518f34-2d3f-4999-bdc4-272c397a8286',
        '2862f31e-cff8-403f-abf1-019725475435',
        'e39722b5-50c4-4394-bc05-9edf7a4e1557',
        '5670b442-a00a-49f1-9ccd-f5dedce94359',
        'b11f65c7-d9a3-4fd0-b23c-edf0869ac4dc',
        'f776c5ea-2395-4a2f-9c64-87c1da2cf8a7',
        '8d93fda4-17f6-4aa3-bb68-bae000cca6dc',
        '3ee6637c-2151-4e08-aab7-65d1fc694ca1',
        '3c1038ad-84a1-404b-897e-6d9fd5683265',
        '33e28982-1a2b-48fe-ae63-6f892f4bea89',
        '8ffa90bb-c51d-4292-b4c2-e4cc62236f42',
        '5c3d0252-f26e-462e-9682-8dab0673189b',
        '37cb24f2-10aa-4c4b-a2d6-7ea47d2d8ea3',
        '2527608b-803f-4bc8-bfcc-11b20f55a4fb',
        '4278bdf5-b5e2-4b72-bf49-b5945fa8492e',
        'c21361aa-35c2-4f87-80dc-1811eecf46eb',
        'a161c55d-eac2-422c-83e6-30eb71480393',
        'af4aa712-beb3-4826-a878-97e7a71d6b13',
        '7eb0b967-93a1-4cef-97ae-d2285cbf36cc',
        '1848624b-bc3e-4b85-a282-8faf80880ce4',
        '32a893c2-1d52-4c62-8830-65cd160beec4',
        'c1155fe1-3aba-4987-ae15-bf18cb591fbd',
        'a1c7b0ab-8363-498f-9a24-29b94c9e1406',
        '0e1b3dcd-0c88-46f8-a6ba-296b28063aee',
        '76dc27c3-3f45-4ed1-a79d-183d18ea9349',
        '50d160cd-1b07-44c3-b8d5-19a1016e50f4',
        '233d9260-951d-4747-8c19-2e47c6c3bb70',
        '62c3cccf-e906-4096-bdd0-3e1a74e75c38',
        '12b6bf7a-eb42-4983-91b1-9c3fa807ff1b'
    ]

    for order_id in order_ids:
        with TaxDetailService() as service:
            tax_detail = service.get_tax_detail(order_id)
            if tax_detail:
                logger.info(f"Successfully built tax detail for order {order_id}")
                # tax_detail_dict = tax_detail.__dict__.copy()
                #
                # # 简单的JSON序列化
                # def default_serializer(obj):
                #     if isinstance(obj, (date, datetime)):
                #         return obj.isoformat()
                #     elif isinstance(obj, Enum):
                #         return obj.value
                #     return str(obj)

                # json_str = json.dumps(tax_detail_dict, default=default_serializer, indent=2, ensure_ascii=False)
                # logger.info(json_str)
                # 这里可以继续处理tax_detail，比如发送到Vertex API


            else:
                logger.info(f"Failed to build tax detail for order {order_id}")


if __name__ == "__main__":
    main()
