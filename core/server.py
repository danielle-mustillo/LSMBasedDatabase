import os

from aiohttp import web
from core.database import create_simple_database



class Server:
    routes = web.RouteTableDef()

    def __init__(self, instance_id = None, page_size = None):
        # Instance ID is mandatory
        if instance_id is None:
            instance_id = str(os.environ['INSTANCE_ID'])

        # Non Mandatory config
        if page_size is None:
            page_size = os.environ.get("PAGE_SIZE", 10)

        self.db = create_simple_database(instance_id=instance_id, page_size=page_size)

    def get_webapp(self):
        app = web.Application()
        app.add_routes(self.routes)
        return app

    @routes.get('/data/{key}')
    async def get(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        value = self.db.get(key)
        if value is None:
            return web.Response(status=404)
        else:
            return web.Response(text="{}".format(value))


    @routes.put('/data/{key}')
    async def put(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        value = await request.text()
        self.db.update(key, value)
        return web.Response(status=204)


    @routes.delete('/data/{key}')
    async def delete(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        self.db.delete(key)
        return web.Response(status=204)


    @routes.post('/data/{key}')
    async def post(self, request: web.Request) -> web.Response:
        key = request.match_info['key']
        value = await request.text()
        self.db.insert(key, value)
        return web.Response(status=201)


    @routes.get("/health")
    async def health(self, request: web.Request) -> web.Response:
        return web.Response(status=200)



