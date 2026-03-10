import httpx
import logging
from config.config import inference_settings

logger = logging.getLogger(__name__)

def notify_operator(machine_id: str, summary: str) -> None:
    """Send anomaly report to Slack webhook."""
    if not inference_settings.slack_webhook_url:
        return
    payload = {
        "text": (
            f":rotating_light: *Anomaly Detected — Machine `{machine_id}`*\n"
            f"{summary}"
        )
    }
    try:
        httpx.post(inference_settings.slack_webhook_url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")