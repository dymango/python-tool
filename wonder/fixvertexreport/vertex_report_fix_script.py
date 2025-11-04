#!/usr/bin/env python3
import json
import logging
import os
import re,time

import requests
from requests.auth import HTTPBasicAuth

# ===== 配置部分 =====
ES_HOST = "log-es-es-http.monitoring2"
ES_PORT = "9200"
ES_ACTION_INDEX = "action-prod-oms-2025.10.30"
ES_TRACE_INDEX = "trace-2025.10.30"
ES_SIZE = 10

# ===== 认证部分 =====
ES_API_KEY = "TUJSUWZZWUJXTDc2a2ExUHVfMlg6Y1pLd01wY1pST1ctU2RzTFRUMVR4dw=="
ES_USER = "kibana"
ES_PASS = "BxKQ3W2tPtfwXnjx"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

order_ids = []
order_list = []
fixed_log_count = 0

ignore_orders = []
failed_orders = []

def make_auth_headers():
    """根据环境变量生成认证头"""
    headers = {"Content-Type": "application/json"}
    if ES_API_KEY:
        headers["Authorization"] = f"ApiKey {ES_API_KEY}"
    return headers


def make_auth_tuple():
    """生成 Basic Auth 对象"""
    if ES_USER and ES_PASS:
        return HTTPBasicAuth(ES_USER, ES_PASS)
    return None


def fetch_page(index, from_offset, size, order_id=None, error_code=None, id=None):
    ES_URL = f"http://{ES_HOST}:{ES_PORT}/{index}/_search"

    # 如果查 _id，使用 ids 查询，忽略其他参数
    if id:
        query = {
            "query": {
                "ids": {"values": [id]}
            }
        }
    else:
        query = {
            "from": from_offset,
            "size": size,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"app": "tax-service"}},
                        {"term": {"action": "topic:oms-order-event"}}
                    ]
                }
            }
        }
        if order_id:
            query["query"]["bool"]["must"].append({"term": {"context.order_id": order_id}})
        if error_code:
            query["query"]["bool"]["must"].append({"term": {"error_code": error_code}})

    resp = requests.post(
        ES_URL,
        headers=make_auth_headers(),
        auth=make_auth_tuple(),
        json=query,
        timeout=10
    )
    resp.raise_for_status()
    return resp.json()


def main():
    from_offset = 0
    total_fetched = 0

    while True:
        try:
            data = fetch_page(ES_ACTION_INDEX, from_offset, ES_SIZE, None, "VERTEX_REPORT_FAILED")
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            logger.info("No more data available.")
            break

        for h in hits:
            source = h.get("_source", {})
            context = source.get("context", "")
            order_id = context.get("order_id", "")[0]
            doc_id = source.get("id", "")
            if order_id and doc_id:
                order_list.append({"order_id": order_id, "_id": doc_id})
                logger.info(f"add: {len(order_list)}")

        total_fetched += len(hits)
        from_offset += ES_SIZE

    logger.info(f"size: {len(order_list)}")
    logger.info(f"Finished. Total {total_fetched} documents retrieved.")

    for item in order_list:
        order_id = item["order_id"]
        doc_id = item["_id"]
        try:
            data = fetch_page(ES_ACTION_INDEX, 0, 200, order_id, None)
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            break
        hits = data.get("hits", {}).get("hits", [])
        last_warn_log = -1
        last_available_log = -1
        last_complete_log = 9999
        is_hdr_order = False
        for index, h in enumerate(hits):
            source = h.get("_source", {})
            context = source.get("context", "")
            error_code = source.get("error_code", "")
            event = context.get("event", "")[0]
            brand_category = context.get("brand_category", "")[0]
            if event == "COMPLETE":
                last_complete_log = index
                last_available_log = index

            if error_code == "VERTEX_REPORT_FAILED":
                last_warn_log = index

            if event == "PLACE_ORDER" or event == "CANCELED" or event == "ISSUE_CREATED" or event == "ADD_PROMOTION":
                last_available_log = index

            if brand_category == 'WONDER_HDR':
                is_hdr_order = True

        if is_hdr_order:
            if last_warn_log >= last_complete_log and last_warn_log >= last_available_log:
                get_message_and_send_to_tax_service(doc_id, order_id)
            else:
                ignore_orders.append(order_id)
        else:
            if last_warn_log >= last_available_log:
                get_message_and_send_to_tax_service(doc_id, order_id)
            else:
                ignore_orders.append(order_id)

    logger.info(f"done.fetch: {total_fetched},  total: {len(order_list)}, {fixed_log_count} vertex fixed.")
    logger.info(f"ignore orders: {ignore_orders}")
    logger.info(f"failed orders: {failed_orders}")
    logger.info(f"failed orders: {len(failed_orders)}")


def get_message_and_send_to_tax_service(id: str, order_id: str):
    global fixed_log_count
    try:
        data = fetch_page(ES_TRACE_INDEX, 0, 10, None, None, id)
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        return
    hits = data.get("hits", {}).get("hits", [])
    for h in hits:
        source = h.get("_source", {})
        content = source.get("content", "")
        pattern = r'value=(.*?), timestamp='
        matches = re.findall(pattern, content, re.DOTALL)  # re.DOTALL 支持跨行匹配
        if matches:
            try:
                json_data = json.loads(matches[0])
                # send_to_tax_service(order_id, json_data)
                logger.info(f"send data to tax service: {fixed_log_count}")
                fixed_log_count += 1
                # time.sleep(0.1)
            except json.JSONDecodeError as e:
                logger.info(f"JSON解析错误: {e}")
                failed_orders.append(order_id)
        else:
            logger.info(f"JSON解析无数据: {e}")

def send_to_tax_service(order_id: str, payload: dict):
    """发送HTTP POST请求到tax-service"""
    TAX_SERVICE_URL = "https://tax-service.uat-consumer.svc.cluster.local/_sys/kafka/topic/oms-order-event/key/{order_id}/handle"
    TAX_SERVICE_TIMEOUT = 20  # 秒
    url = TAX_SERVICE_URL.format(order_id=order_id)
    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=TAX_SERVICE_TIMEOUT,
            verify=False,  # 如果内部自签证书，可以暂时禁用验证
        )
        if resp.status_code == 200:
            logger.info(f"[OK] Sent message for order {order_id}")
        else:
            logger.warning(f"[WARN] Send failed for order {order_id}, status={resp.status_code}, resp={resp.text}")
    except requests.RequestException as e:
        logger.error(f"[ERROR] Failed to send message for order {order_id}: {e}")

if __name__ == "__main__":
    main()
