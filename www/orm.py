# _*_ coding:utf-8 _*_
import logging,asyncio
import aiomysql

#from orm import Model,StringField,IntegerField

#class user(Model):
#   __table__='users'
#
#   id=IntegerField(primary_key=True)
#   name=StringField()
        
def log(sql,args=()):
    logging.info('SQL:%s' % sql)


#定义一个链接池，后续可以供其它sql语句的方法以协程的方式调用池中的链接
@asyncio.coroutine
def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['root'],
        password=kw['yzyyzyyzy'],
        db=kw['db'],
        charset=kw.get('charset','utf-8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minisize=kw.get('minisize',1),
        loop=loop
    )

#定义select方法，使用yield from获取协程的子程序
@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        cur= yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%s'),args or())
        if size:
            rs=yield from cur.fetchmany(size)
        else:
            rs=yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned:%s' % len(rs))
        return rs
#定义insert/update/delete,因为输入的参数一样，同样是输出一个影响行数
@asyncio.coroutine
def execute(sql,args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur=yield from conn.cursor()
            yield from cur.execute(sql.replace('?','%s'),args)
            affected=cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L=[]
    for n in range(num):
        L.append('?')
    return ','.join(L)

class Field(object):

    def __init__(sefl,name,column_type,primary_key,default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default

    def __str__(self):
        return '<%s,%s:%s>' %(self.__class__.__name__,self.column_type,self.name)

#创建五种数据库字段的类型，都是Field的子类
class StringField(Field)   
    def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)

class BooleanField(Fied):
    def __init__(self,name=None,default=False):
        super.__init__(name,'boolean',False,default)

class IntegerField(Field):
    def __init__(self,name=None,primary_key=False,default=0):
        super.__init__(name,'bigint',primary_key,default)

class FloatField(Field):
    def __init__(self,name=None,primary_key=False,default=0.0):
        super.__init__(name.'real',primary_key,default)

class TextField(Field):
    def __init__(self,name=None,default=None):
        super.__init__(name,'text',False,default)

class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        tableName=attrs.get('__table__',None) or name
        logging.info('found Model: %s (table:%s)' % (name,tableName))
        mappings=dict()
        fields=[]
        primaryKey=None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('found mapping:%s ==> %s' % (k,v))
                mapping[k]=v
                if v.primary_key:
                    #找到主键：
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field:%s' % k)
                    primaryKey=k
                else:
                    fields.append(k)
        if not primaryKey:
            raise StandardError('primary key not found.')
        for k in mappings.key():
            attrs.pop(k)
        escaped_fields=list(map(lambda f:'`%s`' % f,fields))
        attrs['__mappings__']=mappings #保存属性和列的映射关系
        attrs['__table__']=tableName
        attrs['__primary_key__']=primary_key#主键属性名
        attrs['__fields__']=fields#除主键以后的属性名
        attrs['__select__']='select `%s`,%s from`%s`' % (primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__']='insert into `%s`(%s,`%s`) values (%s)' % (tableName,','join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
        attrs['__update__']='update `%s` set %s where `%s`=?' % (tableName,','join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__']='delete from `%s` where `%s`=?' % (tableName,primaryKey)
        return type.__new__(cls,name,bases,attrs)
        
        
        


        


