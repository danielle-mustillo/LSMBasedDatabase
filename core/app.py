from aiohttp import web

from core.server import Server

def run(port: int = 8921):
    server = Server()
    web.run_app(server.get_webapp(), port=port)


if __name__ == '__main__':
    run()
