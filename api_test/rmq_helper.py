import asyncio
import aio_pika
import uuid
from aio_pika import connect, IncomingMessage, Message
import environ
import functools
import pickle
import time
import threading
env = environ.Env()

def serialize_request(req):
    sreq = {
        'body': req.body,
        'GET': dict(req.GET),
        'POST': dict(req.POST),
        'COOKIES': dict(req.COOKIES),
        'remote_addr': req.META.get('REMOTE_ADDR'),
        'headers': dict(req.headers),
        'path': req.path,
        'path_info': req.path_info,
        'method': req.method,
        'content_type': req.content_type,
        'content_params': req.content_params
    }
    return sreq

class RPCClient:
    def __init__(self,loop):
        self.loop = loop
        self.futures = {}
    async def init(self):
        self.connection = await aio_pika.connect_robust(env("RABBITMQ_URL"))
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(auto_delete=True)
        await self.callback_queue.consume(self.on_response)
    async def on_response(self, message):
        with message.process():
            body = pickle.loads(message.body)
            future = self.futures.pop(message.correlation_id)
            future.set_result(body['response'])
    async def call_rpc_uf(self,*args,**kwargs):
        fut = asyncio.run_coroutine_threadsafe(self.call_rpc(*args,**kwargs),self.loop)
        return fut.result(30)
    async def call_rpc(self,request,backend_name,endpoint,**params):
        sreq = serialize_request(request)
        corr_id = str(uuid.uuid4())
        self.futures[corr_id] =  self.loop.create_future()
        exchange = await self.channel.get_exchange(backend_name, ensure=True)
        await self.callback_queue.bind(exchange,self.callback_queue.name)
        await exchange.publish(
            Message(
                body=pickle.dumps({
                    'request':sreq,
                    'params':params,
                    'endpoint':endpoint
                }),
                correlation_id=corr_id,
                reply_to=self.callback_queue.name
            ),
            routing_key="rpc_queue"
        )
        return await self.futures[corr_id]

rpcclient = None
class RPCWrapper:
    _instance = None
    @staticmethod
    def get_instance():
        if RPCWrapper._instance is None:
            RPCWrapper._instance = RPCWrapper()
        return RPCWrapper._instance
    def __init__(self):
        self.ready = False
        def __init_rpcclient_t(self):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.rpcclient = RPCClient(loop)
            loop.run_until_complete(self.rpcclient.init())
            self.ready = True
            loop.run_forever()
        t = threading.Thread(target=__init_rpcclient_t,args=(self,))
        t.start()

        while not self.ready:
            time.sleep(0.1)

    @staticmethod
    def call_rpc(request,backend_name,endpoint,**params):
        rpcclient = RPCWrapper.get_instance().rpcclient
        return functools.partial(rpcclient.call_rpc_uf,request,backend_name,endpoint,**params) 