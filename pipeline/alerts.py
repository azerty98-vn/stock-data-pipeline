"""on_failure_callback dùng chung cho các task ingest.

Fail cứng + alert (thay vì skip-and-continue) cho lỗi fetch: thiếu dữ liệu
1 ngày ảnh hưởng trực tiếp tới moving average/volatility ở mart layer
(rolling window tính sai nếu thiếu 1 điểm), nên đây phải là lỗi chặn DAG
và báo ngay, không phải warning-rồi-tiếp-tục.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def alert_on_failure(context: dict) -> None:
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exception = context.get("exception")
    message = f"[{dag_id}] task `{task_id}` failed: {exception}"
    logger.error(message)

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    import requests

    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except requests.RequestException:
        logger.exception("Failed to send Slack alert")
