import asyncio
from uuid import uuid4
from aioapns import APNs, NotificationRequest, PushType

def notifier(token, title, message, category):
    request = NotificationRequest(
        device_token=token,
        message = {
            "aps": {
                "alert": message,
                "title": title,
                "payload": {"messageFrom": category},
            }
        },
    )

    return request

async def run():
    apns_cert_client = APNs(
        client_cert='certificates.pem',
        use_sandbox=True,
    )

    stats_sent = 0
    stats_success = 0
    stats_error = 0

    while True:
        rows = [
            "b9597c02545ecc074bf321540001d1d2b6689b5353fc01b4284ae28607788746"
        ]

        print("...")

        if len(rows) == 0:
            await asyncio.sleep(10)
            continue

        # sending notifications
        for row in rows:
            print(row)

            stats_sent += 1

            request = notifier(row, "hello", "world", "root")
            response = await apns_cert_client.send_notification(request)

            print(response.status, response.description)

loop = asyncio.get_event_loop()
loop.create_task(run())
loop.run_forever()

