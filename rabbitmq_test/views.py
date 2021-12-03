from django.http import HttpResponse
from api_test.rmq_helper import RPCWrapper
import json

async def index(request):
    res = await RPCWrapper.call_rpc(request,"hello_world","echo_back")(text="Echo back")

    return HttpResponse(json.dumps(res))