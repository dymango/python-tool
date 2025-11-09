from vertex_order_detail_model import *


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


@dataclass
class TaxCategory:
    id: str
    taxCategory: str
    taxSubCategory: str
    taxDriverCode: str
    isActive: bool


@dataclass
class Seller:
    company: str
    physicalOrigin: 'PhysicalOrigin'


@dataclass
class Currency:
    isoCurrencyCodeAlpha: str
    isoCurrencyName: str
    isoCurrencyCodeNum: int


@dataclass
class PhysicalOrigin:
    mainDivision: str
    city: str
    postalCode: str
    subDivision: Optional[str] = None
    streetAddress1: Optional[str] = None


@dataclass
class Customer:
    @dataclass
    class CustomerCode:
        value: str

    @dataclass
    class Destination:
        mainDivision: str
        city: str
        subDivision: str
        postalCode: str
        streetAddress1: str

    customerCode: CustomerCode
    destination: Destination


@dataclass
class Product:
    productClass: str
    value: Optional[str] = None


@dataclass
class FlexibleCodeField:
    fieldId: int
    value: str


@dataclass
class FlexibleFields:
    flexibleCodeFields: List[FlexibleCodeField] = field(default_factory=list)


@dataclass
class LineItem:
    lineItemId: str
    extendedPrice: float
    product: Product
    flexibleFields: FlexibleFields


@dataclass
class ReportTaxParam:
    saleMessageType: str = "INVOICE"
    customer: Optional[Customer] = None
    documentDate: str = ""
    documentNumber: str = ""
    locationCode: Optional[str] = None
    seller: Optional[Seller] = None
    lineItems: List[LineItem] = field(default_factory=list)


@dataclass
class CalculateTaxRequest:
    saleMessageType: str
    transactionType: str = "SALE"
    currency: Optional[Currency] = None
    customer: Optional[Customer] = None
    documentDate: str = ""
    documentNumber: str = ""
    documentType: Optional[str] = None
    locationCode: Optional[str] = None
    seller: Optional[Seller] = None
    lineItems: List[LineItem] = field(default_factory=list)


@dataclass
class CalculateTaxResult:
    documentDate: str
    lineItems: List['CalculateTaxResult.LineItem']
    totalTax: float

    @dataclass
    class LineItem:
        lineItemId: str
        extendedPrice: float
        taxes: List['CalculateTaxResult.Tax']
        totalTax: float
        product: Product

        def total_tax_rate(self):
            return sum(tax.effectiveRate for tax in self.taxes)

        def taxable(self):
            if any(tax.taxable > 0 for tax in self.taxes):
                return max(tax.taxable for tax in self.taxes)
            return max(abs(tax.taxable) for tax in self.taxes)

    @dataclass
    class Tax:
        calculatedTax: float
        effectiveRate: float
        taxable: float
        inclusionRuleId: Optional[str] = None
        calculationRuleId: Optional[str] = None


# 常量
class TaxConstants:
    BA_DELIVERY_FEE_CATEGORY_ID = "ba_delivery_fee_category"

    @staticmethod
    def all_fee_tax_category_ids():
        return ["service_fee", "delivery_fee", "fast_pass_fee"]


class FeeType:
    SERVICE_FEE = "SERVICE_FEE"
    DELIVERY_FEE = "DELIVERY_FEE"
    FAST_PASS_FEE = "FAST_PASS_FEE"


class TaxCategoryIds:
    BA_DELIVERY_FEE = "TC-BA_DELIVERY_FEE"
