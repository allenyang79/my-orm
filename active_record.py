import os
import sys
import weakref
import pymongo
from mongobox import MongoBox

box = MongoBox()
box.start()
db_client = box.client()  # pymongo client
db = db_client['test']

classes = {}

class ActiveRecordMeta(type):

    def __new__(meta_cls, cls_name, cls_bases, cls_dict):
        if cls_name in classes:
            raise Exception('`%s` is existd.' % cls_name)
        cls = type.__new__(meta_cls, cls_name, cls_bases, cls_dict)
        classes[cls_name] = cls
        return cls

class ActiveRecord(object):
    __metaclass__ = ActiveRecordMeta
    __table__ = None

    @classmethod
    def create(cls, attrs):
        """Create new one and save in db immediate.
        """
        instance = cls(attrs)
        instance.save()
        return instance

    @classmethod
    def find(cls, query={}):
        for row in db[cls.__table__].find(query):
            yield cls(row)

    @classmethod
    def find_one(cls, query={}):
        row = next(db[cls.__table__].find(query), None)
        if row:
            row = cls(row)
        return row

    #@classmethod
    # def generate_id():
    #    return

    def __init__(self, attrs):
        """Create new instance without save in db.
        It is better to create new ont by classmethod ``create``.

        """
        self.attrs = attrs

    def get_id(self):
        if not self.is_new():
            return self.attrs['_id']
        else:
            raise Exception('no `_id` in this model instance.')

    def is_new(self):
        if '_id' not in self.attrs:
            return True
        return False

    def save(self):
        """Save instance.
        """
        cls = type(self)
        attrs = self.attrs

        if self.is_new():  # '_id' not in self.attr:
            result = db[self.__table__].insert_one(self.attrs)
            self.attrs['_id'] = result.inserted_id
        else:
            result = db[self.__table__].find_one_and_update({'_id': self.attrs['_id']}, {
                '$set': self.attrs
            }, return_document=pymongo.ReturnDocument.AFTER)
            self.attrs.update(result)
        return self


class HasMany(object):
    def __init__(self, field, cls_name):
        self.field = field
        self.cls_name = cls_name
        self.values = weakref.WeakKeyDictionary()

    def __get__(self, instance, cls):
        if not instance in self.values:
            self.values[instance] = _HasMany(instance, self.field, self.cls_name)
        return self.values[instance]

    def __set__(self, instance, value):
        raise Exception('can set on a `HasMany` field.')


class _HasMany(object):
    """ Save local_ref_key in attrs of local instance. and query this ref_id
    """

    def __init__(self, instance, field, cls_name):
        self.instance = instance
        self.field = field
        self.cls_name = cls_name

    def add(self, val):
        cls = classes[self.cls_name]
        if not isinstance(val, cls):
            raise Exception('val is not a `%s`' % self.cls_name)
        if self.field not in self.instance.attrs[self.field]:
            self.instance.attrs[self.field].append(val.get_id())
        self.instance.attrs[self.field].append(val.get_id())
        self.instance.attrs[self.field] = list(set(self.instance.attrs[self.field]))

    def remove(self, val):
        _id = val.get_id()
        if _id in self.instance.sttrs[self.field]:
            self.instance.attrs[self.field].remove(_id)
            return True
        return False

    def fetch(self):
        cls = classes[self.cls_name]
        return cls.find({
            '_id': {'$in': self.instance.attrs[self.field]}
        })


class BelongTo(object):
    def __init__(self, field, cls_name):
        self.field = field
        self.cls_name = cls_name
        self.values = weakref.WeakKeyDictionary()

    def __get__(self, instance, cls):
        if not instance in self.values:
            self.values[instance] = _BelongTo(instance, self.field, self.cls_name)
        return self.values[instance]

    def __set__(self, instance, value):
        raise Exception('can set on a `BelongTo` field.')


class _BelongTo(object):
    def __init__(self, instance, field, cls_name):
        self.instance = instance
        self.field = field
        self.cls_name = cls_name

    def set(self, val):
        cls = classes[self.cls_name]
        if not isinstance(val, cls):
            raise Exception('val is not a `%s`' % self.cls_name)
        self.instance.attrs[self.field] = val.get_id()

    def delete(self, val):
        _id = val.get_id()
        del self.instance.attrs[self.field]

    def get(self):
        cls = classes[self.cls_name]
        return cls.find_one({
            '_id': self.instance.attrs[self.field]
        })


if __name__ == '__main__':
    class Employee(ActiveRecord):
        __table__ = 'employees'
        departments = HasMany('departments', 'Department')

        def __init__(self, init_dict={}):
            self.attrs = init_dict

        def __str__(self):
            return '%s(%s)' % (type(self).__name__, self.attrs)

    class Department(ActiveRecord):
        __table__ = 'departments'
        company = BelongTo('company', 'Company')

    class Company(ActiveRecord):
        __table__ = 'companies'

    john = Employee.create({
        'name': 'allen',
        'phone': '0988',
        'address': 'this is my address',
        'departments': []
    })

    sales_dep = Department.create({
        'name': 'sales department',
        'location': '12F'
    })

    company = Company.create({
        'name': 'company-name'
    })

    print john.attrs
    print 'john', john.get_id()
    print "========"
    john.attrs['address'] = 'new address'
    john.save()
    print john.attrs

    john.attrs['departments'] = [0, 1, 2, 3]
    john.save()
    john.attrs['hello'] = john.get_id()
    john.save()
    print john.attrs
    print "========"

    john.departments.add(sales_dep)
    john.save()
    print john.attrs
    print "========="
    print "departments"
    for row in john.departments.fetch():
        print row

    print "========="
    print company
    sales_dep.company.set(company)
    print sales_dep.attrs
    sales_dep.save()
