import asyncio
import redis
import json
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

async def run(r):
    apns_cert_client = APNs(
        client_cert='certificates.pem',
        use_sandbox=True,
    )

    stats_sent = 0
    stats_success = 0
    stats_error = 0

    while True:
        row = r.blpop("notificator", 10)

        if row == None:
            continue

        print(row)

        stats_sent += 1

        try:
            data = json.loads(row[1])
            request = notifier(data["token"], data["title"], data["message"], data["category"])

            response = await apns_cert_client.send_notification(request)
            print(response.status, response.description)

        except Exception as e:
            print(e)
            pass


r = redis.Redis("10.241.0.240")

loop = asyncio.get_event_loop()
loop.create_task(run(r))
loop.run_forever()

