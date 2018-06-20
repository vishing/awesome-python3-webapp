# _*_ coding:utf-8 _*_
import logging; logging.basicConfig(level=logging.INFO)
import asyncio,os,json,time 
from datetime import datetime
from aiohttp import web

#定义首页返回页面,content_type要指定，才能打开页面，否则会下载。
def index(request):
    return web.Response(body=b'<h1>Awesome</h1>',content_type='text/html')

#装饰器的作用是定义该函数是协程，多用于迭代的产生器
@asyncio.coroutine
def init(loop):
    #定义一个webapp
    app=web.Application(loop=loop)
    #添加一个get方法，后续可以添加post
    app.router.add_route('GET','/',index)
    #用生成器方式创建一个网络服务
    srv=yield from loop.create_server(app.make_handler(),'127.0.0.1',9000)
    logging.info('server started at http://127.0.0.1:9000')
    return srv

loop=asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()