from decimal import Decimal
import os

class OverviewAnalytics:
    # This class is to provide high-level overview statistics \
    # from the electricity records.
    # It will receive items from a DynamoDB table \
    # and analyse them for result display in the website.

    def __init__(self, items) -> None:
        # items are output from a DynamoDB table.

        self.items = items

    def summary(self):
        # To analyse the electricity records from DynamoDB.
        # Return the data that will be transfered to frontend.

        items = self.items
        total_records = len(items)
        device_ids = {it["device_id"] for it in items} if items else set()
        device_count = len(device_ids)
        latest_period = max((int(it["period_no"]) for it in items), default=0)
        alerts_total = sum(1 for it in items if it.get("alert_flag") is True)

        # The sum of alerts of latest 1,440 period
        low = max(0, latest_period - 1440 + 1)
        alerts_last_1440_period = sum(
            1 for it in items if it.get("alert_flag") is True
            and int(it["period_no"]) >= low
        )

        body = {
            "device_count": device_count,
            "total_records": total_records,
            "latest_period": latest_period,
            "alerts_last_1440_period": alerts_last_1440_period,
            "alerts_total": alerts_total,
        }

        return body