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

#用于生成?,?,?,的格式
def create_args_string(num):
    L=[]
    for n in range(num):
        L.append('?')
    return ','.join(L)

#创建基类，Field
class Field(object):
    #初始化基类，包括属性的名字、属性的类型、主键和默认值
    def __init__(sefl,name,column_type,primary_key,default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default
    #初始化SQL相关的显示语句。格式为：<属性的名字，属性的类型：类名或表名>
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

#创建model的元数据类，用于将具体的子类与数据库表的映射信息读取出来
class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        #需要排除model类本身，要不然会报错，因为model并不存在数据库的表中
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        #获取表名
        tableName=attrs.get('__table__',None) or name
        logging.info('found Model: %s (table:%s)' % (name,tableName))
        #获取所有的Field和主键名:
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
        #构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__']='select `%s`,%s from`%s`' % (primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__']='insert into `%s`(%s,`%s`) values (%s)' % (tableName,','join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
        attrs['__update__']='update `%s` set %s where `%s`=?' % (tableName,','join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__']='delete from `%s` where `%s`=?' % (tableName,primaryKey)
        return type.__new__(cls,name,bases,attrs)

#创建基类MODEL，用于做表对象的父类        
class Model(dict,metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
    #get方法定义，内部获取数据
    def __getattr__(self,key):
        try:
            return self[key]
        except keyError:
            raise AttributeError(r"'Model object has no attribute '%s'" % key)
    #set方法定义，内部存储数据
    def __setattr__(self,key,value):
        self[key]=value
    #外部根据KEY获取数据
    def getValue(self,key):
        return getattr(self,key,None)
    #获取某个属性的值，如果该对象的该属性还没有赋值，就去获取它对应的列的默认值
    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            fileld=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable (field.default) else field.default
                logging.debug('using default value for %s:%s' % (key,str(value)))
                seattr(self,key,value)
        return value
    @classmethod
    async def findAll(cls,where=None,args=None,**kw):
        'find objects by where clause.'
        sql=[cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args=[]
        orderBy= kw.get('orderBy',None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit=kw.get('limit',None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit,int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit,tuple) and len(limit)==2:
                sql.append('?','?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value:%s' % str(limit))
        rs= await select(' '.join(sql),args)
        return[cls(**r) for r in rs]
    
    @classmethod
    async def findNumber(cls,selectField,where=None,args=None):
        sql=['select %s _num_ from `%s`' % (selectField,cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs= await select(''.join(sql),args,1)
        if len(rs)==0:
            return None
        return rs[0]['_num_']


    @classmethod
    async def find(cls,pk):
        rs = await select('%s where `%s`=?' % (cls.__select__,cls.__primary_key__),[pk],1)
        if len(rs)==0:
            return None
        return cls(**rs[0])

    async def save(self):
        args=list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows=await execute(self.__insert__,args)
        if rows !=1:
            logging.warn('failed to insert record:affected rows:%s' % rows)

    async def update(self):
        args=list(map(self.getValue,self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows=await execute(self.__update__,args)
        if rows !=1:
            logging.warn('failed to update by primary key:affected rows:%s' % rows)

    async def remove(self):
        args=[self.getValue(self.__primary_key__)]
        rows=await execute(self.__delete__,args)
        if rows !=1:
            logging.warn('failed to remove by primary_key: affected rows:%s' % rows)


        


