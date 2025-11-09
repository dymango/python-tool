# models.py

import logging
# vertex_item_tax_context.py
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN, ROUND_DOWN, ROUND_UP
from typing import Dict

import mysql.connector
import requests

from model import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db_config = {
    'host': 'rfprodv2-flexible-wonder-db-replica-v4.mysql.database.azure.com',
    'user': 'datadog',
    'password': 'jPT8Q#gL9XLo%6ls',
    'database': 'tax'
}

success_send_to_os_orders = []
failed_send_to_os_orders = []

@dataclass
class VertexReportEventMessage:
    document_number: str
    order_number: Optional[str] = None
    order_id: Optional[str] = None
    action: str = "INVOICE"
    item_taxes: List['VertexReportEventMessage.ReportItemTax'] = field(default_factory=list)

    @dataclass
    class ReportItemTax:
        item_id: str
        item_price: Decimal
        item_tax: Decimal
        item_tax_rate: Decimal
        taxable: Decimal
        fee_taxes: List['VertexReportEventMessage.ReportFeeTax'] = field(default_factory=list)
        tax_rule_ids: List[str] = field(default_factory=list)
        total_tax: Decimal = Decimal('0')

    @dataclass
    class ReportFeeTax:
        fee_type: str
        fee: Decimal
        fee_tax: Decimal
        taxable: Decimal
        fee_tax_rate: Decimal


# 常量类
class VertexReportAction:
    INVOICE = "INVOICE"


class FeeType:
    SERVICE_FEE = "SERVICE_FEE"
    DELIVERY_FEE = "DELIVERY_FEE"
    FAST_PASS_FEE = "FAST_PASS_FEE"
    SMALL_ORDER_FEE = "SMALL_ORDER_FEE"
    HOSPITALITY_FEE = "HOSPITALITY_FEE"


class TaxConstants:
    BA_DELIVERY_FEE_CATEGORY_ID = "ba_delivery_fee_category"

    @staticmethod
    def tax_category_id(fee_type: str) -> str:
        return f"TC-{fee_type}"

    @staticmethod
    def all_fee_tax_category_ids() -> List[str]:
        fee_types = [FeeType.SERVICE_FEE, FeeType.DELIVERY_FEE, FeeType.FAST_PASS_FEE]
        fee_tax_category_ids = [TaxConstants.tax_category_id(fee_type) for fee_type in fee_types]
        fee_tax_category_ids.append(TaxConstants.BA_DELIVERY_FEE_CATEGORY_ID)
        return fee_tax_category_ids


class TaxCategoryIds:
    BA_DELIVERY_FEE = "TC-BA_DELIVERY_FEE"


class TaxCategoryRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def get_tax_categories(self, ids: List[str]) -> List[TaxCategory]:
        """根据ID列表获取税务类别"""
        if not ids:
            return []

        placeholders = ','.join(['%s'] * len(ids))
        query = f"""
        SELECT id, tax_category, tax_sub_category, tax_driver_code, is_active
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
                taxCategory=row['tax_category'],
                taxSubCategory=row['tax_sub_category'],
                taxDriverCode=row['tax_driver_code'],
                isActive=bool(row['is_active'])
            )
            for row in results
        ]


@dataclass
class RequestItem:
    itemId: str
    itemPrice: Decimal
    fees: List['Fee'] = field(default_factory=list)


@dataclass
class Fee:
    feeType: str
    feeAmount: Decimal


@dataclass
class ItemTax:
    itemId: str
    itemPrice: Decimal
    itemTax: Decimal
    itemTaxRate: Decimal
    itemTaxable: Decimal
    feeTaxes: List['FeeTax'] = field(default_factory=list)


@dataclass
class FeeTax:
    feeType: str
    feeAmount: Decimal
    tax: Decimal
    taxRate: Decimal
    taxable: Decimal


class VertexFeeSharedCalculator:
    def __init__(self, vertex_fee_line_items: List[CalculateTaxResult.LineItem],
                 brand_category: str, item_fees: List['ItemFee'], fee_tax_categories: List[TaxCategory]):
        self.vertex_fee_line_items = {item.lineItemId: item for item in vertex_fee_line_items}
        self.brand_category = brand_category
        self.item_fees = item_fees
        self.fee_tax_category_map = {tax_cat.id: tax_cat for tax_cat in fee_tax_categories}

    def calculate(self) -> List['ItemFee']:
        """计算共享费用"""
        deliver_fees = [fee for fee in self.item_fees if fee.feeType == FeeType.DELIVERY_FEE]
        fast_pass_fees = [fee for fee in self.item_fees if fee.feeType == FeeType.FAST_PASS_FEE]

        self._calculate_delivery_fee(deliver_fees)
        self._calculate_fast_pass_fee(fast_pass_fees)

        result = []
        result.extend(deliver_fees)
        result.extend(fast_pass_fees)
        return result

    def _calculate_delivery_fee(self, deliver_fees: List['ItemFee']):
        """计算配送费"""
        if not deliver_fees:
            return

        fee_tax_category_id = TaxCategoryIds.BA_DELIVERY_FEE if self.brand_category == "BLUE_APRON" else TaxConstants.tax_category_id(
            FeeType.DELIVERY_FEE)
        tax_category = self.fee_tax_category_map.get(fee_tax_category_id)
        if not tax_category:
            return

        line_item_id = tax_category.taxDriverCode
        self._calculate_item_fee(line_item_id, deliver_fees)

    def _calculate_fast_pass_fee(self, fast_pass_fees: List['ItemFee']):
        """计算快速通道费"""
        if not fast_pass_fees:
            return

        fee_tax_category_id = TaxConstants.tax_category_id(FeeType.FAST_PASS_FEE)
        tax_category = self.fee_tax_category_map.get(fee_tax_category_id)
        if not tax_category:
            return

        line_item_id = tax_category.taxDriverCode
        self._calculate_item_fee(line_item_id, fast_pass_fees)

    def _calculate_item_fee(self, line_item_id: str, item_fees: List['ItemFee']):
        """计算项目费用"""
        fee_line_item = self.vertex_fee_line_items.get(line_item_id)
        if not fee_line_item:
            return

        fee_total = sum(item_fee.fee for item_fee in item_fees)
        if fee_total == Decimal('0'):
            return

        total_tax = Decimal(str(fee_line_item.totalTax))
        taxable = Decimal(str(fee_line_item.taxable()))
        tax_rate = Decimal(str(fee_line_item.total_tax_rate()))

        remain_fee = total_tax
        remain_fee_taxable = taxable

        for i, item_fee in enumerate(item_fees):
            item_fee.feeTaxRate = tax_rate

            if i != len(item_fees) - 1:
                percentage = item_fee.fee / fee_total
                fee_tax_by_percent = (total_tax * percentage).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                item_fee.feeTax = remain_fee if abs(fee_tax_by_percent) > abs(remain_fee) else fee_tax_by_percent

                fee_taxable_by_percent = (taxable * percentage).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                item_fee.taxable = remain_fee_taxable if abs(fee_taxable_by_percent) > abs(
                    remain_fee_taxable) else fee_taxable_by_percent
            else:
                item_fee.feeTax = remain_fee
                item_fee.taxable = remain_fee_taxable

            remain_fee -= item_fee.feeTax
            remain_fee_taxable -= item_fee.taxable

    @dataclass
    class ItemFee:
        itemId: str
        feeType: str
        fee: Decimal
        feeTax: Decimal = Decimal('0')
        taxable: Decimal = Decimal('0')
        feeTaxRate: Decimal = Decimal('0')


class VertexItemTaxContext:
    def __init__(self, request_items: List[RequestItem], brand_category: str,
                 calculate_tax_result: CalculateTaxResult, fee_tax_categories: List[TaxCategory]):
        self.brand_category = brand_category
        self.calculate_tax_result = calculate_tax_result
        self.request_items = request_items
        self.fee_tax_categories = fee_tax_categories
        self.item_taxes: List[ItemTax] = []

    def calculate_item_taxes(self):
        """计算项目税费 - 根据Java逻辑完整实现"""
        # 获取通用费用行项目（配送费和快速通道费）
        common_fee_line_items = self._get_fee_line_items(
            self.calculate_tax_result.lineItems,
            [FeeType.DELIVERY_FEE, FeeType.FAST_PASS_FEE]
        )

        # 计算共享项目费用
        shared_item_fees = self._calculate_shared_item_fees(common_fee_line_items)

        # 获取服务费行项目
        service_fee_line_items = self._get_fee_line_items(
            self.calculate_tax_result.lineItems,
            [FeeType.SERVICE_FEE]
        )

        # 计算每个项目的税费
        self.item_taxes = []
        for request_item in self.request_items:
            item_tax = ItemTax(
                itemId=request_item.itemId,
                itemPrice=request_item.itemPrice,
                itemTax=Decimal('0'),
                itemTaxRate=Decimal('0'),
                itemTaxable=Decimal('0'),
                feeTaxes=[]
            )

            # 处理主要行项目
            main_line_item = self._get_main_line_item(request_item)
            if main_line_item:
                item_tax.itemPrice = Decimal(str(main_line_item.extendedPrice))
                item_tax.itemTax = Decimal(str(main_line_item.totalTax))
                item_tax.itemTaxRate = Decimal(str(main_line_item.total_tax_rate()))
                item_tax.itemTaxable = Decimal(str(main_line_item.taxable()))

            # 处理费用税费
            if request_item.fees:
                for fee in request_item.fees:
                    if fee.feeType in [FeeType.SMALL_ORDER_FEE, FeeType.HOSPITALITY_FEE]:
                        fee_tax = self._default_fee_tax(fee)
                    elif fee.feeType == FeeType.SERVICE_FEE:
                        fee_tax = self._build_service_fee_tax(request_item, fee, service_fee_line_items)
                    else:
                        fee_tax = self._build_common_fee_tax(request_item, fee, shared_item_fees)

                    item_tax.feeTaxes.append(fee_tax)

            self.item_taxes.append(item_tax)

    def _get_fee_line_items(self, line_items: List[CalculateTaxResult.LineItem], fee_types: List[str]) -> List[
        CalculateTaxResult.LineItem]:
        """获取费用行项目"""
        fee_tax_driver_codes = {
            self._get_fee_tax_category_driver_code(fee_type)
            for fee_type in fee_types
        }

        result = []
        for line_item in line_items:
            if not line_item.product:
                continue

            product_class = line_item.product.productClass or ""
            product_value = line_item.product.value or ""

            if (product_class in fee_tax_driver_codes or
                    product_value in fee_tax_driver_codes):
                result.append(line_item)

        return result

    def _get_fee_tax_category_driver_code(self, fee_type: str) -> str:
        """获取费用税务类别驱动代码"""
        if fee_type == FeeType.DELIVERY_FEE and self.brand_category == "BLUE_APRON":
            tax_category_id = TaxCategoryIds.BA_DELIVERY_FEE
        else:
            tax_category_id = TaxConstants.tax_category_id(fee_type)

        for tax_category in self.fee_tax_categories:
            if tax_category.id == tax_category_id:
                return tax_category.taxDriverCode

        return fee_type

    def _calculate_shared_item_fees(self, common_fee_line_items: List[CalculateTaxResult.LineItem]) -> List[
        VertexFeeSharedCalculator.ItemFee]:
        """计算共享项目费用"""
        item_fees = []
        for request_item in self.request_items:
            if request_item.fees:
                for fee in request_item.fees:
                    if fee.feeType in [FeeType.DELIVERY_FEE, FeeType.FAST_PASS_FEE]:
                        item_fee = VertexFeeSharedCalculator.ItemFee(
                            itemId=request_item.itemId,
                            feeType=fee.feeType,
                            fee=fee.feeAmount,
                            feeTax=Decimal('0'),
                            feeTaxRate=Decimal('0'),
                            taxable=Decimal('0')
                        )
                        item_fees.append(item_fee)

        calculator = VertexFeeSharedCalculator(
            common_fee_line_items,
            self.brand_category,
            item_fees,
            self.fee_tax_categories
        )
        return calculator.calculate()

    def _get_main_line_item(self, request_item: RequestItem) -> Optional[CalculateTaxResult.LineItem]:
        """获取主要行项目"""
        for line_item in self.calculate_tax_result.lineItems:
            if (line_item.product and
                    line_item.lineItemId == request_item.itemId and
                    (not line_item.product.value or line_item.product.value == "")):
                return line_item
        return None

    def _build_common_fee_tax(self, request_item: RequestItem, fee: Fee,
                              shared_item_fees: List[VertexFeeSharedCalculator.ItemFee]) -> FeeTax:
        """构建通用费用税费"""
        for item_fee in shared_item_fees:
            if (item_fee.feeType == fee.feeType and
                    item_fee.itemId == request_item.itemId):
                return FeeTax(
                    feeType=fee.feeType,
                    feeAmount=fee.feeAmount,
                    tax=item_fee.feeTax,
                    taxRate=item_fee.feeTaxRate,
                    taxable=item_fee.taxable
                )

        return self._default_fee_tax(fee)

    def _build_service_fee_tax(self, request_item: RequestItem, fee: Fee,
                               service_fee_line_items: List[CalculateTaxResult.LineItem]) -> FeeTax:
        """构建服务费税费"""
        fee_tax_category_driver_code = self._get_fee_tax_category_driver_code(fee.feeType)

        for service_fee_line_item in service_fee_line_items:
            if (service_fee_line_item.lineItemId == request_item.itemId and
                    service_fee_line_item.product.value == fee_tax_category_driver_code):
                return FeeTax(
                    feeType=fee.feeType,
                    feeAmount=fee.feeAmount,
                    tax=Decimal(str(service_fee_line_item.totalTax)),
                    taxRate=Decimal(str(service_fee_line_item.total_tax_rate())),
                    taxable=Decimal(str(service_fee_line_item.taxable()))
                )

        return self._default_fee_tax(fee)

    def _default_fee_tax(self, fee: Fee) -> FeeTax:
        """默认费用税费"""
        return FeeTax(
            feeType=fee.feeType,
            feeAmount=fee.feeAmount,
            tax=Decimal('0'),
            taxRate=Decimal('0'),
            taxable=Decimal('0')
        )

    def get_item_taxes(self) -> List[ItemTax]:
        """获取项目税费列表"""
        return self.item_taxes.copy()


class VertexReportEventMessagePublisher:
    def __init__(self):
        self.tax_category_repo = TaxCategoryRepository(mysql.connector.connect(**db_config))

    def publish_vertex_invoice_report_event_message(self,
                                                    document_number: str,
                                                    tax_detail: OMSOrderEventMessageTaxDetail,
                                                    brand_category: str,
                                                    calculate_tax_result: CalculateTaxResult) -> bool:
        """发布Vertex发票报告事件消息"""
        try:
            logger.info(f"Starting Vertex report publishing for document: {document_number}")

            # 1. 计算项目税费
            item_taxes = self._calculate_item_taxes(tax_detail, brand_category, calculate_tax_result)

            # 2. 构建消息
            message = self._build_message(document_number, tax_detail, brand_category, item_taxes, calculate_tax_result)

            # 3. 发送到order-service
            success = self._send_to_order_service(message)
            if success:
                logger.info(f"✅ Successfully published Vertex report for document {document_number}")
            else:
                logger.error(f"❌ Failed to publish Vertex report for document {document_number}")

            logger.info(f"success order: {len(success_send_to_os_orders)}, failed order: {len(failed_send_to_os_orders)}")
            return True

        except Exception as e:
            logger.error(f"Error publishing Vertex report message for {document_number}: {e}")
            return False

    def _calculate_item_taxes(self, tax_detail: OMSOrderEventMessageTaxDetail,
                              brand_category: str,
                              calculate_tax_result: CalculateTaxResult) -> List:
        """计算项目税费"""
        try:
            # 获取费用税务类别
            fee_tax_category_ids = TaxConstants.all_fee_tax_category_ids()
            fee_tax_categories = self.tax_category_repo.get_tax_categories(fee_tax_category_ids)

            # 构建请求项目
            request_items = []
            for item in tax_detail.order_taxable_charge_items:
                request_item = self._build_request_item(item)
                request_items.append(request_item)

            # 计算税费
            vertex_context = VertexItemTaxContext(
                request_items=request_items,
                brand_category=brand_category,
                calculate_tax_result=calculate_tax_result,
                fee_tax_categories=fee_tax_categories
            )
            vertex_context.calculate_item_taxes()

            return vertex_context.get_item_taxes()

        except Exception as e:
            logger.error(f"Error calculating item taxes: {e}")
            return []

    def _build_request_item(self, order_item: OrderTaxableChargeItem) -> RequestItem:
        """构建请求项目"""
        # 使用外部定义的RequestItem类
        request_item = RequestItem(
            itemId=order_item.item_id,
            itemPrice=Decimal(str(order_item.taxable_subtotal))
        )

        # 添加各种费用
        fees_config = [
            (order_item.taxable_service_fee, FeeType.SERVICE_FEE),
            (order_item.taxable_fast_pass_fee, FeeType.FAST_PASS_FEE),
            (order_item.taxable_delivery_fee, FeeType.DELIVERY_FEE),
            (order_item.taxable_small_order_fee, FeeType.SMALL_ORDER_FEE)
        ]

        for fee_amount, fee_type in fees_config:
            if fee_amount and fee_amount > 0:
                # 使用外部定义的Fee类
                fee = Fee(
                    feeType=fee_type,
                    feeAmount=Decimal(str(fee_amount))
                )
                request_item.fees.append(fee)

        return request_item

    def _build_message(self, document_number: str,
                       tax_detail: OMSOrderEventMessageTaxDetail,
                       brand_category: str,
                       item_taxes: List[ItemTax],
                       calculate_tax_result: CalculateTaxResult) -> VertexReportEventMessage:
        """构建消息"""
        # 构建项目税费列表
        msg_item_taxes = []
        for line_item in tax_detail.order_taxable_charge_items:
            # 查找对应的税费计算结果
            item_tax_result = next(
                (tax for tax in item_taxes if tax.itemId == line_item.item_id),
                None
            )

            if item_tax_result:
                tax_rule_ids = self._get_tax_rule_ids(line_item.item_id, calculate_tax_result)
                report_item_tax = self._build_report_item_tax(item_tax_result, tax_rule_ids)
            else:
                report_item_tax = self._default_report_item_tax(line_item)

            msg_item_taxes.append(report_item_tax)
        message = VertexReportEventMessage(
            document_number=document_number,
            item_taxes=msg_item_taxes,
            action='INVOICE')
        # 设置订单ID或订单号
        if self._is_ba_subscription_order(brand_category, tax_detail.schedule_type.name):
            message.order_id = document_number
        else:
            message.order_number = document_number


        return message

    def _get_tax_rule_ids(self, item_id: str, result: CalculateTaxResult) -> List[str]:
        """获取税务规则ID"""
        tax_rule_ids = set()

        for line_item in result.lineItems:
            if line_item.lineItemId == item_id:
                for tax in line_item.taxes:
                    if tax.inclusionRuleId:
                        tax_rule_ids.add(tax.inclusionRuleId)
                    if tax.calculationRuleId:
                        tax_rule_ids.add(tax.calculationRuleId)

        return list(tax_rule_ids)

    def _build_report_item_tax(self, item_tax, tax_rule_ids: List[str]) -> VertexReportEventMessage.ReportItemTax:
        """构建报告项目税费"""
        report_item_tax = VertexReportEventMessage.ReportItemTax(
            item_id=item_tax.itemId,
            item_price=item_tax.itemPrice,
            item_tax=item_tax.itemTax,
            item_tax_rate=item_tax.itemTaxRate,
            taxable=item_tax.itemTaxable,
            tax_rule_ids=tax_rule_ids
        )

        # 构建费用税费
        report_item_tax.fee_taxes = []
        if hasattr(item_tax, 'feeTaxes') and item_tax.feeTaxes:
            for fee_tax in item_tax.feeTaxes:
                report_fee_tax = VertexReportEventMessage.ReportFeeTax(
                    fee_type=fee_tax.feeType,
                    fee=fee_tax.feeAmount,
                    fee_tax=fee_tax.tax,
                    taxable=fee_tax.taxable,
                    fee_tax_rate=fee_tax.taxRate
                )
                report_item_tax.fee_taxes.append(report_fee_tax)

        # 计算总税费
        total_tax = item_tax.itemTax
        for fee_tax in report_item_tax.fee_taxes:
            total_tax += fee_tax.fee_tax
        report_item_tax.total_tax = total_tax

        return report_item_tax

    def _default_report_item_tax(self, line_item: OrderTaxableChargeItem) -> VertexReportEventMessage.ReportItemTax:
        """默认报告项目税费"""
        report_item_tax = VertexReportEventMessage.ReportItemTax(
            item_id=line_item.item_id,
            item_price=Decimal(str(line_item.taxable_subtotal)),
            item_tax=Decimal('0'),
            item_tax_rate=Decimal('0'),
            taxable=Decimal('0'),
            tax_rule_ids=[],
            fee_taxes=[]
        )

        # 添加默认费用税费
        fees_config = [
            (line_item.taxable_service_fee, FeeType.SERVICE_FEE),
            (line_item.taxable_fast_pass_fee, FeeType.FAST_PASS_FEE),
            (line_item.taxable_delivery_fee, FeeType.DELIVERY_FEE),
            (line_item.taxable_small_order_fee, FeeType.SMALL_ORDER_FEE)
        ]

        for fee_amount, fee_type in fees_config:
            if fee_amount and fee_amount > 0:
                report_fee_tax = VertexReportEventMessage.ReportFeeTax(
                    fee_type=fee_type,
                    fee=Decimal(str(fee_amount)),
                    fee_tax=Decimal('0'),
                    taxable=Decimal('0'),
                    fee_tax_rate=Decimal('0')
                )
                report_item_tax.fee_taxes.append(report_fee_tax)

        # 计算总税费
        total_tax = Decimal('0')
        for fee_tax in report_item_tax.fee_taxes:
            total_tax += fee_tax.fee_tax
        report_item_tax.total_tax = total_tax

        return report_item_tax

    def _is_ba_subscription_order(self, brand_category: str, schedule_type: str) -> bool:
        """检查是否为BA订阅订单"""
        return brand_category == "BLUE_APRON" and schedule_type == "SUBSCRIPTION"

    def _send_to_order_service(self, message: VertexReportEventMessage) -> bool:
        """发送消息到order-service"""
        try:
            # 转换为字典
            message_dict = self._message_to_dict(message)

            logger.info(f"message body: {message_dict}")

            ORDER_SERVICE_URL = "https://order-service.prod-consumer.svc.cluster.local/_sys/kafka/topic/vertex-report-event/key/{document_number}/handle"
            url = ORDER_SERVICE_URL.format(document_number=message_dict['document_number'])

            try:
                resp = requests.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json=message_dict,
                    timeout=20,
                    verify=False,
                )
                if resp.status_code == 200:
                    logger.info(f"[OK] Sent message for order")
                else:
                    logger.warning(f"[WARN] Send failed for order, status={resp.status_code}, resp={resp.text}")
                success_send_to_os_orders.append(message.document_number)
            except requests.RequestException as e:
                logger.error(f"[ERROR] Failed to send message for order: {e}")
                failed_send_to_os_orders.append(message.document_number)
                return False

            return True
        except Exception as e:
            logger.error(f"❌ Error sending Vertex report to order-service: {e}")
            return False


    def _message_to_dict(self, message: VertexReportEventMessage) -> Dict:
        """将消息转换为字典"""

        def decimal_default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        message_dict = {
            "document_number": message.document_number,
            "action": message.action,
            "item_taxes": []
        }

        if message.order_id:
            message_dict["order_id"] = message.order_id
        if message.order_number:
            message_dict["order_number"] = message.order_number
        #
        it = message.item_taxes
        for t in it:
            item_tax_dict = {
                "item_id": t.item_id,
                "item_price": float(t.item_price),
                "item_tax": float(t.item_tax),
                "item_tax_rate": float(t.item_tax_rate),
                "taxable": float(t.taxable),
                "total_tax": float(t.total_tax),
                "tax_rule_ids": t.tax_rule_ids,
                "fee_taxes": []
            }

            for fee_tax in t.fee_taxes:
                fee_tax_dict = {
                    "fee_type": fee_tax.fee_type,
                    "fee": float(fee_tax.fee),
                    "fee_tax": float(fee_tax.fee_tax),
                    "taxable": float(fee_tax.taxable),
                    "fee_tax_rate": float(fee_tax.fee_tax_rate)
                }
                item_tax_dict["fee_taxes"].append(fee_tax_dict)

            message_dict["item_taxes"].append(item_tax_dict)

        return message_dict
