import json
import logging
from typing import Dict

import mysql.connector
import requests

import send_msg_to_order_service
from model import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = 'eyJ4NXQjUzI1NiI6IkJoVmtSbk5ZODgyY3BNTFhGbkN4SzRNbXA3eVJlR25zQUd3MzBDclVfR2siLCJraWQiOiJhMDEzYTI5MC1hYTc2LTRmYzItOGQzNi1iN2ZjZGJlNjMxZmMiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI5ZWY1MWMxNjEwZWMudmVydGV4aW5jLmNvbSIsImF1ZCI6IiIsIm5iZiI6MTc2MjcxMDIxMiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDk1L29zZXJpZXMtYXV0aCIsImV4cCI6MTc2MjcxMjAxMiwiaWF0IjoxNzYyNzEwMjEyLCJ1c2VySWQiOjQwLCJqdGkiOiJjODUzM2Q1OC03YjIzLTRjODktYjM4Ny05NDQ1MjEwYWJhNDEifQ.i6gTzF4ralAHWnF1m3wCEi4-SY2_5HRFPNNDqIt2jfaM4k3-xzR9a1Ck-zayD63NofwM63-ZK8o-iRnTc5ko7jlYItpRNUczTjTbaega-YScBA97t7Fw840ItWjwIyHXaAtdI_npANZVnlrgcPmppFPRkmSgub3_JD4JU92IeOJccziZSMOfUvAJBz0UpcWM154GPa1JmUIhCq3W-4xd3ig37O8k5JfTiQXiYdhxmajJEHvPGHAC7CaRo_82CdSZGuS69mg-VsB41Pv2ojfUnjrQue_zgxUh3N94YCPLESevZYrpjWSq5tSg6rnBNMj6ZjY-MFFdyKxW0mPk3o5KtA'

# 配置
class VertexConfig:
    BASE_URL = "https://grubhub2.na1.ondemand.vertexinc.com"
    CLIENT_ID = "9ef51c1610ec.vertexinc.com"
    CLIENT_SECRET = "e28e4b7ee7c8e69486d5a75830723843183be6b3412b9f0f3b95037f1266880d"
    TRANSACTION_URL = f"{BASE_URL}/vertex-ws/v2/supplies"


db_config = {
    'host': 'rfprodv2-flexible-wonder-db-replica-v4.mysql.database.azure.com',
    'user': 'datadog',
    'password': 'jPT8Q#gL9XLo%6ls',
    'database': 'tax'
}


# 数据模型 - 全部使用驼峰命名法


# Vertex Helper - 更新方法以使用新的字段名
class VertexHelper:
    BA_COMPANY = "BA1"
    HDR_COMPANY = "HDR1"

    @staticmethod
    def seller(company: str, physicalOrigin: PhysicalOrigin) -> Seller:
        return Seller(company=company, physicalOrigin=physicalOrigin)

    @staticmethod
    def customer(customerId: str, stateCode: str, city: str, county: str, zipCode: str,
                 addressLine: str) -> Customer:
        customerCode = Customer.CustomerCode(value=customerId)
        destination = Customer.Destination(
            mainDivision=stateCode,
            city=city,
            subDivision=county,
            postalCode=zipCode,
            streetAddress1=addressLine
        )
        return Customer(customerCode=customerCode, destination=destination)

    @staticmethod
    def product(productClass: str, value: Optional[str] = None) -> Product:
        return Product(productClass=productClass, value=value)

    @staticmethod
    def flexible_code_field(fieldId: int, value: str) -> FlexibleCodeField:
        return FlexibleCodeField(fieldId=fieldId, value=value)

    @staticmethod
    def physical_origin(stateCode: str, city: str, county: str, zipCode: str, addressLine: str) -> PhysicalOrigin:
        return PhysicalOrigin(
            mainDivision=stateCode,
            city=city,
            postalCode=zipCode,
            subDivision=county,
            streetAddress1=addressLine
        )

    @staticmethod
    def get_blue_apron_physical_origin(facilityCode: Optional[str]) -> PhysicalOrigin:
        if facilityCode and facilityCode.strip():
            # 这里需要根据facilityCode获取具体的地址信息
            # 暂时使用默认地址
            return VertexHelper.default_ba_order_physical_origin()
        return VertexHelper.default_ba_order_physical_origin()

    @staticmethod
    def default_ba_order_physical_origin() -> PhysicalOrigin:
        return PhysicalOrigin(
            mainDivision="NJ",
            city="Linden",
            postalCode="07036",
            streetAddress1="901 W Linden Ave"
        )


# 数据库访问类
class TaxCategoryRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def get_tax_categories(self, ids: List[str]) -> List[TaxCategory]:
        """根据ID列表获取税务类别"""
        if not ids:
            return []

        placeholders = ','.join(['%s'] * len(ids))
        query = f"""
        SELECT id, tax_category as taxCategory, tax_sub_category as taxSubCategory, 
               tax_driver_code as taxDriverCode, is_active as isActive
        FROM tax_categories 
        WHERE id IN ({placeholders}) AND is_active = TRUE
        """

        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, ids)
        results = cursor.fetchall()
        cursor.close()

        return [
            TaxCategory(
                id=row['id'],
                taxCategory=row['taxCategory'],
                taxSubCategory=row['taxSubCategory'],
                taxDriverCode=row['taxDriverCode'],
                isActive=bool(row['isActive'])
            )
            for row in results
        ]


# Vertex API 客户端 - 更新以处理驼峰命名
class VertexAPIClient:
    def __init__(self, config: VertexConfig):
        self.config = config
        self.session = requests.Session()

    def calculate_tax(self, token: str, tokenType: str, request: CalculateTaxRequest) -> Dict:
        """计算税务"""
        url = f"{self.config.TRANSACTION_URL}"
        headers = {
            "Authorization": f"{tokenType} {token}",
            "Content-Type": "application/json"
        }

        try:
            # 直接使用dataclass的__dict__，因为字段名已经是驼峰命名
            request_dict = self._dataclass_to_camel_dict(request)
            response = self.session.post(url, json=request_dict, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to calculate tax: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise

    def _dataclass_to_camel_dict(self, obj):
        """将dataclass对象转换为字典，保持驼峰命名"""
        if hasattr(obj, '__dict__'):
            result = {}
            for key, value in obj.__dict__.items():
                if not key.startswith('_'):
                    # 字段名已经是驼峰命名，直接使用
                    result[key] = self._dataclass_to_camel_dict(value)
            return result
        elif isinstance(obj, list):
            return [self._dataclass_to_camel_dict(item) for item in obj]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        else:
            return str(obj)


# 主要服务类 - 更新以使用新的字段名
class VertexService:
    def __init__(self, vertex_client: VertexAPIClient, tax_category_repo: TaxCategoryRepository):
        self.vertex_client = vertex_client
        self.tax_category_repo = tax_category_repo

    def get_document_number(self, brand_category, orderId: str, orderNumber: str, tax_detail) -> str:
        """获取文档编号"""
        if brand_category == "WONDER_HDR":
            return orderNumber
        return orderNumber if tax_detail.schedule_type.name == "ONE_TIME_PURCHASE" else orderId

    def get_location_code(self, brand_category, tax_detail) -> Optional[str]:
        """获取位置代码"""
        if not tax_detail.ship_from:
            return None
        if brand_category == "BLUE_APRON":
            return tax_detail.ship_from.facility_code
        if brand_category == "WONDER_HDR":
            return tax_detail.ship_from.hdr_name
        return None

    def get_orders_source(self, order_channel) -> Optional[str]:
        """获取订单来源"""
        if not order_channel:
            return None

        order_source_map = {
            "UBER_EATS": "1P on 3P",
            "SEAMLESS": "1P on 3P",
            "GRUB_HUB": "1P on 3P",
            "DOOR_DASH": "1P on 3P",
            "CAVIAR": "1P on 3P",
            "POSTMATES": "1P on 3P",
            "BA_APP": "BA - App",
            "BA_WEB": "BA - Web",
            "APP": "1P - App",
            "WEB": "1P - Web",
            "IN_PERSON": "1P - POP"
        }

        return order_source_map.get(order_channel.value if hasattr(order_channel, 'value') else order_channel)

    def get_tax_driver_code(self, feeType: str, brandCategory: str, feeTaxCategories: List[TaxCategory]) -> str:
        """获取税务驱动代码"""
        if feeType == FeeType.DELIVERY_FEE and brandCategory == "BLUE_APRON":
            ba_delivery_category = next(
                (tax_cat for tax_cat in feeTaxCategories if tax_cat.id == TaxConstants.BA_DELIVERY_FEE_CATEGORY_ID),
                None
            )
            if ba_delivery_category:
                return ba_delivery_category.taxDriverCode

        # 查找匹配的税务类别
        matching_category = next(
            (tax_cat for tax_cat in feeTaxCategories if tax_cat.taxSubCategory == feeType),
            None
        )

        return matching_category.taxDriverCode if matching_category else feeType

    def build_flexible_fields(self, tax_detail, item) -> FlexibleFields:
        """构建灵活字段"""
        flexible_fields = FlexibleFields()
        orders_source = self.get_orders_source(tax_detail.order_channel)

        if orders_source:
            flexible_fields.flexibleCodeFields.append(
                VertexHelper.flexible_code_field(1, orders_source)
            )

        if item and hasattr(item, 'bundle_id') and item.bundle_id and item.bundle_id.strip():
            flexible_fields.flexibleCodeFields.append(
                VertexHelper.flexible_code_field(2, "B")
            )

        if tax_detail.need_utensils:
            flexible_fields.flexibleCodeFields.append(
                VertexHelper.flexible_code_field(4, "Utensils")
            )

        flexible_fields.flexibleCodeFields.append(
            VertexHelper.flexible_code_field(3, "TBC Off Premise")
        )

        return flexible_fields

    def build_product(self, taxCategory: TaxCategory) -> Product:
        """构建产品信息"""
        if not taxCategory or not taxCategory.taxDriverCode:
            return None
        return VertexHelper.product(taxCategory.taxDriverCode)

    def build_line_item(self, tax_detail, itemId: str, feeAmount: float, feeTaxDriverCode: str,
                        itemTaxDriverCode: str) -> LineItem:
        """构建行项目"""
        return LineItem(
            lineItemId=itemId,
            extendedPrice=feeAmount,
            product=VertexHelper.product(itemTaxDriverCode, feeTaxDriverCode),
            flexibleFields=self.build_flexible_fields(tax_detail, None)
        )

    def build_line_item_for_fee(self, tax_detail, feeTaxDriverCode: str, totalFee: float) -> LineItem:
        """为费用构建行项目"""
        return LineItem(
            lineItemId=feeTaxDriverCode,
            extendedPrice=totalFee,
            product=VertexHelper.product(feeTaxDriverCode),
            flexibleFields=self.build_flexible_fields(tax_detail, None)
        )

    def build_service_fee_line_items(self, brandCategory: str, tax_detail, reportItems: List,
                                     itemTaxCategoryMap: Dict[str, TaxCategory],
                                     feeTaxCategories: List[TaxCategory]) -> List[LineItem]:
        """构建服务费行项目"""
        line_items = []
        service_fee_tax_driver_code = self.get_tax_driver_code(FeeType.SERVICE_FEE, brandCategory, feeTaxCategories)

        if not service_fee_tax_driver_code:
            return line_items

        for item in reportItems:
            if item.menu_item_tax_category_id not in itemTaxCategoryMap:
                continue
            if hasattr(item, 'taxable_service_fee') and item.taxable_service_fee and item.taxable_service_fee > 0:
                item_tax_driver_code = itemTaxCategoryMap[item.menu_item_tax_category_id].taxDriverCode
                line_items.append(
                    self.build_line_item(tax_detail, item.item_id, item.taxable_service_fee,
                                         service_fee_tax_driver_code, item_tax_driver_code)
                )

        return line_items

    def build_common_fee_line_items(self, brandCategory: str, tax_detail, reportItems: List,
                                    feeTaxCategories: List[TaxCategory]) -> List[LineItem]:
        """构建通用费用行项目"""
        line_items = []

        # 计算总配送费
        total_delivery_fee = sum(
            item.taxable_delivery_fee for item in reportItems
            if hasattr(item, 'taxable_delivery_fee') and item.taxable_delivery_fee and item.taxable_delivery_fee > 0
        )

        # 计算总快速通道费
        total_fast_pass_fee = sum(
            item.taxable_fast_pass_fee for item in reportItems
            if hasattr(item, 'taxable_fast_pass_fee') and item.taxable_fast_pass_fee and item.taxable_fast_pass_fee > 0
        )

        if total_delivery_fee > 0:
            delivery_tax_driver_code = self.get_tax_driver_code(FeeType.DELIVERY_FEE, brandCategory,
                                                                feeTaxCategories)
            line_items.append(
                self.build_line_item_for_fee(tax_detail, delivery_tax_driver_code, total_delivery_fee)
            )

        if total_fast_pass_fee > 0:
            fast_pass_tax_driver_code = self.get_tax_driver_code(FeeType.FAST_PASS_FEE, brandCategory,
                                                                 feeTaxCategories)
            line_items.append(
                self.build_line_item_for_fee(tax_detail, fast_pass_tax_driver_code, total_fast_pass_fee)
            )

        return line_items

    def build_report_tax_param(self, documentNumber: str, userId: str, brandCategory: str,
                               tax_detail, itemTaxCategoryMap: Dict[str, TaxCategory],
                               feeTaxCategories: List[TaxCategory]) -> ReportTaxParam:
        """构建报告税务参数"""
        param = ReportTaxParam()
        param.saleMessageType = "INVOICE"
        param.customer = VertexHelper.customer(
            userId,
            tax_detail.state_code,
            tax_detail.city,
            tax_detail.county,
            tax_detail.zip_code,
            tax_detail.address_line
        )
        param.documentDate = tax_detail.service_date.isoformat() if hasattr(tax_detail.service_date,
                                                                            'isoformat') else str(
            tax_detail.service_date)
        param.documentNumber = documentNumber
        param.locationCode = self.get_location_code(brandCategory, tax_detail)

        # 设置seller
        if brandCategory == "BLUE_APRON":
            facility_code = tax_detail.ship_from.facility_code if tax_detail.ship_from else None
            param.seller = VertexHelper.seller(
                VertexHelper.BA_COMPANY,
                VertexHelper.get_blue_apron_physical_origin(facility_code)
            )
        else:
            if tax_detail.ship_from:
                param.seller = VertexHelper.seller(
                    VertexHelper.HDR_COMPANY,
                    VertexHelper.physical_origin(
                        tax_detail.ship_from.state_code,
                        tax_detail.ship_from.city,
                        tax_detail.ship_from.county,
                        tax_detail.ship_from.zip_code,
                        tax_detail.ship_from.address_line
                    )
                )

        # 构建主要行项目
        param.lineItems = [
            LineItem(
                lineItemId=item.item_id,
                extendedPrice=item.taxable_subtotal,
                product=self.build_product(itemTaxCategoryMap[item.menu_item_tax_category_id]),
                flexibleFields=self.build_flexible_fields(tax_detail, item)
            )
            for item in tax_detail.order_taxable_charge_items
            if item.menu_item_tax_category_id in itemTaxCategoryMap
        ]

        # 添加服务费行项目
        param.lineItems.extend(
            self.build_service_fee_line_items(brandCategory, tax_detail, tax_detail.order_taxable_charge_items,
                                              itemTaxCategoryMap, feeTaxCategories)
        )

        # 添加通用费用行项目
        param.lineItems.extend(
            self.build_common_fee_line_items(brandCategory, tax_detail, tax_detail.order_taxable_charge_items,
                                             feeTaxCategories)
        )

        return param

    def build_calculate_tax_request(self, param: ReportTaxParam) -> CalculateTaxRequest:
        """构建计算税务请求"""
        # 截断locationCode
        locationCode = param.locationCode[:20] if param.locationCode else None

        return CalculateTaxRequest(
            saleMessageType=param.saleMessageType,
            transactionType="SALE",
            currency=Currency("USD", "US Dollar", 840),
            customer=param.customer,
            documentDate=param.documentDate,
            documentNumber=param.documentNumber,
            locationCode=locationCode,
            seller=param.seller,
            lineItems=param.lineItems
        )

    def build_calculate_tax_result(self, response_data: Dict) -> CalculateTaxResult:
        """构建计算税务结果"""
        data = response_data.get('data', {})

        lineItems = []
        for line_item_data in data.get('lineItems', []):
            taxes = []
            for tax_data in line_item_data.get('taxes', []):
                tax = CalculateTaxResult.Tax(
                    calculatedTax=tax_data.get('calculatedTax', 0),
                    effectiveRate=tax_data.get('effectiveRate', 0),
                    taxable=tax_data.get('taxable', 0),
                    inclusionRuleId=tax_data.get('inclusionRuleId', {}).get('value') if tax_data.get(
                        'inclusionRuleId') else None,
                    calculationRuleId=tax_data.get('calculationRuleId', {}).get('value') if tax_data.get(
                        'calculationRuleId') else None
                )
                taxes.append(tax)

            lineItem = CalculateTaxResult.LineItem(
                lineItemId=line_item_data.get('lineItemId'),
                extendedPrice=line_item_data.get('extendedPrice', 0),
                taxes=taxes,
                totalTax=line_item_data.get('totalTax', 0),
                product=Product(
                    productClass=line_item_data.get('product', {}).get('productClass'),
                    value=line_item_data.get('product', {}).get('value')
                )
            )
            lineItems.append(lineItem)

        return CalculateTaxResult(
            documentDate=data.get('documentDate'),
            lineItems=lineItems,
            totalTax=data.get('totalTax', 0)
        )

    def report_tax(self, param: ReportTaxParam) -> CalculateTaxResult:
        """报告税务到Vertex"""
        calculate_tax_request = self.build_calculate_tax_request(param)

        response_data = self.vertex_client.calculate_tax(
            TOKEN,
            'Bearer',
            calculate_tax_request
        )

        return self.build_calculate_tax_result(response_data)

    def report_order(self, documentNumber: str, userId: str, brandCategory: str, tax_detail) -> Optional[
        CalculateTaxResult]:
        """报告订单到Vertex"""
        if not tax_detail:
            logger.info("Tax detail is None, skipping Vertex report")
            return None

        try:
            # 获取项目税务类别
            item_tax_category_ids = [item.menu_item_tax_category_id for item in tax_detail.order_taxable_charge_items]
            item_tax_categories = self.tax_category_repo.get_tax_categories(item_tax_category_ids)
            item_tax_category_map = {tax_cat.id: tax_cat for tax_cat in item_tax_categories}

            # 获取费用税务类别
            fee_tax_categories = self.tax_category_repo.get_tax_categories(TaxConstants.all_fee_tax_category_ids())

            param = self.build_report_tax_param(
                documentNumber, userId, brandCategory, tax_detail, item_tax_category_map, fee_tax_categories
            )

            if param.lineItems:
                result = self.report_tax(param)
                logger.info(f"Successfully reported tax to Vertex for document {documentNumber}")
                publisher = send_msg_to_order_service.VertexReportEventMessagePublisher()
                publisher.publish_vertex_invoice_report_event_message(documentNumber, tax_detail, brandCategory, result)
                return result
            else:
                logger.info(f"No line items to report for document {documentNumber}")
                return None

        except Exception as e:
            logger.error(f"Failed to report tax to Vertex for document {documentNumber}: {e}")
            return None


def report_main(orderId: str, userId: str, brandCategory: str, tax_detail: OMSOrderEventMessageTaxDetail):
    # Vertex配置
    vertex_config = VertexConfig()

    # 建立数据库连接
    db_connection = mysql.connector.connect(**db_config)

    try:
        # 初始化服务
        tax_category_repo = TaxCategoryRepository(db_connection)
        vertex_client = VertexAPIClient(vertex_config)
        vertex_service = VertexService(vertex_client, tax_category_repo)

        if tax_detail:
            # 获取文档编号
            document_number = vertex_service.get_document_number(
                brandCategory,
                orderId,
                tax_detail.order_number,
                tax_detail
            )

            result = vertex_service.report_order(document_number, userId, brandCategory, tax_detail)

            if result:
                logger.info("✅ Successfully reported order to Vertex")
            else:
                logger.error("❌ Failed to report order to Vertex")

    finally:
        db_connection.close()
