#!/usr/bin/env python3
import json
import logging
import os
import re
import time
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd  # ✅ 新增

# ===== 配置部分 =====
ES_HOST = "log-es-es-http.monitoring2"
ES_PORT = "9200"
ES_ACTION_INDEX = "action-prod-oms-2025.10.29"
ALL_INDEX = "action-prod-oms-*"
ES_TRACE_INDEX = "trace-*"
ES_SIZE = 10

# ===== 认证部分 =====
# uat  YnV3a2JvWUJ0Z0drVWdYMl9JNWM6ZzJBNmpTc01TVXVPTDB1Ui0wQ0dlQQ==   rRSczeTFcz
# prod  TUJSUWZZWUJXTDc2a2ExUHVfMlg6Y1pLd01wY1pW1wY1pST1ctU2RzTFRUMVR4dw==   BxKQ3W2tPtfwXnjx
ES_API_KEY = "TUJSUWZZWUJXTDc2a2ExUHVfMlg6Y1pLd01wY1pW1wY1pST1ctU2RzTFRUMVR4dw=="
ES_USER = "kibana"
ES_PASS = "BxKQ3W2tPtfwXnjx"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

order_list = []
fixed_log_count = 0
ignore_orders = []
failed_orders = []
success_orders = []
failed_order_ids = []


def make_auth_headers():
    headers = {"Content-Type": "application/json"}
    if ES_API_KEY:
        headers["Authorization"] = f"ApiKey {ES_API_KEY}"
    return headers


def make_auth_tuple():
    if ES_USER and ES_PASS:
        return HTTPBasicAuth(ES_USER, ES_PASS)
    return None


def fetch_page(index, from_offset, size, order_id=None, error_code=None, id=None):
    ES_URL = f"http://{ES_HOST}:{ES_PORT}/{index}/_search"

    if id:
        query = {"query": {"ids": {"values": [id]}}}
    else:
        query = {
            "from": from_offset,
            "size": size,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"app": "tax-service"}},
                        {"term": {"action": "topic:oms-order-event"}},
                    ]
                }
            },
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
        timeout=10,
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
            context = source.get("context", {})
            order_id = context.get("order_id", [""])[0]
            event = context.get("event", [""])[0]
            doc_id = source.get("id", "")
            if order_id and doc_id:
                order_list.append({"order_id": order_id, "_id": doc_id, "event": event})
                logger.info(f"add: {len(order_list)}")

        total_fetched += len(hits)
        from_offset += ES_SIZE

    logger.info(f"size: {len(order_list)}")
    logger.info(f"Finished. Total {total_fetched} documents retrieved.")

    for item in order_list:
        order_id = item["order_id"]
        doc_id = item["_id"]
        orderEvent = item["event"]
        try:
            data = fetch_page(ALL_INDEX, 0, 200, order_id, None)
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
            context = source.get("context", {})
            error_code = source.get("error_code", "")
            event = context.get("event", [""])[0]
            brand_category = context.get("brand_category", [""])[0]
            if event == "COMPLETE":
                last_complete_log = index
                last_available_log = index
            if error_code == "VERTEX_REPORT_FAILED":
                last_warn_log = index
            if event in ["PLACE_ORDER", "CANCELED", "ISSUE_CREATED", "ADD_PROMOTION"]:
                last_available_log = index
            if brand_category == "WONDER_HDR":
                is_hdr_order = True

        if is_hdr_order:
            if last_warn_log >= last_complete_log and last_warn_log >= last_available_log:
                get_message_and_send_to_tax_service(doc_id, order_id, orderEvent)
            else:
                ignore_orders.append(order_id)
        else:
            if last_warn_log >= last_available_log:
                get_message_and_send_to_tax_service(doc_id, order_id, orderEvent)
            else:
                ignore_orders.append(order_id)

    logger.info(f"done.fetch: {total_fetched},  total: {len(order_list)}, {fixed_log_count} vertex fixed.")
    # logger.info(f"ignore orders: {ignore_orders}")
    logger.info(f"success orders: {success_orders}")
    # logger.info(f"failed orders: {failed_orders}")
    logger.info(f"failed orders count: {len(failed_orders)}")
    logger.info(f"failed orderids: {failed_order_ids}")

    # ✅ 输出到 Excel
    if failed_orders:
        df = pd.DataFrame(failed_orders)
        output_file = os.path.abspath("failed_orders.xlsx")
        df.to_excel(output_file, index=False)
        logger.info(f"✅ failed_orders 已输出到 {output_file}")
    else:
        logger.info("✅ 无 failed_orders，无需生成 Excel 文件。")


def get_message_and_send_to_tax_service(id: str, order_id: str, event: str):
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
        pattern = r"value=(.*?), timestamp="
        matches = re.findall(pattern, content, re.DOTALL)
        if matches:
            try:
                json_data = json.loads(matches[0])
                # send_to_tax_service(order_id, json_data)
                logger.info(f"send data to tax service: {fixed_log_count}")
                fixed_log_count += 1
                success_orders.append(order_id)
                # time.sleep(0.1)
            except json.JSONDecodeError as e:
                logger.info(f"JSON解析错误: {e}")
                failed_orders.append({"order_id": order_id, "event": event})  # ✅ 修正为字典格式'
                failed_order_ids.append(order_id)
        else:
            logger.info("JSON解析无数据")
            failed_orders.append({"order_id": order_id, "event": event})  # ✅ 补充
            failed_order_ids.append(order_id)


def send_to_tax_service(order_id: str, payload: dict):
    TAX_SERVICE_URL = "https://tax-service.prod-consumer.svc.cluster.local/_sys/kafka/topic/oms-order-event/key/{order_id}/handle"
    TAX_SERVICE_TIMEOUT = 20
    url = TAX_SERVICE_URL.format(order_id=order_id)
    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=TAX_SERVICE_TIMEOUT,
            verify=False,
        )
        if resp.status_code == 200:
            logger.info(f"[OK] Sent message for order {order_id}")
        else:
            logger.warning(f"[WARN] Send failed for order {order_id}, status={resp.status_code}, resp={resp.text}")
    except requests.RequestException as e:
        logger.error(f"[ERROR] Failed to send message for order {order_id}: {e}")


if __name__ == "__main__":
    main()
