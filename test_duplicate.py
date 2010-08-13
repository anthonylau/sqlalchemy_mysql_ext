#! /usr/bin/env python

# Copyright 2010 Brian Edwards. All Rights Reserved.

import unittest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
import duplicate

class TestInsertOnDuplicate(unittest.TestCase):

    def setUp(self):
        engine = create_engine('mysql+mysqldb://duplicate@localhost/duplicate_test', 
                               paramstyle='pyformat')
        self.conn = engine.connect()
        meta = MetaData(engine)
        self.table = Table('users', meta,
            Column('id', Integer, unique=True),
            Column('name', String(25)))
        self.table.create()

    def tearDown(self):
        self.table.drop()
        self.table = None
        self.conn = None

    def testInsert(self):
        self.insertFetchAssert(42, 'Avery')
        self.insertFetchAssert(42, 'Sophie')

    def insertFetchAssert(self, id, name):
        ins = self.table.insertOnDuplicate().\
                           values(id=id, name=name)
        self.conn.execute(ins)
        self.assertEqual(self.fetchName(id), name)

    def fetchName(self, id):
        c = self.table.c
        sel = select([c.name]).\
                where(c.id==id).\
                limit(1)
        return self.conn.execute(sel).scalar()

if __name__ == '__main__':
    unittest.main()
