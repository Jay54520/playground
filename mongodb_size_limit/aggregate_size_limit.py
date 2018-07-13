# -*- coding: utf-8 -*-
"""
The aggregate command can return either a cursor or store the results in a collection.
When returning a cursor or storing the results in a collection,
each document in the result set is subject to the BSON Document Size limit,
currently 16 megabytes; if any single document that exceeds the BSON Document Size limit,
the command will produce an error.
The limit only applies to the returned documents;
during the pipeline processing, the documents may exceed this size.
The db.collection.aggregate() method returns a cursor by default.
"""

# 是 aggregate result 的总大小还是 result 中的单个文件不能超过限制？
# 创建文件，一个 10 MB，一个 10 MB，这样就能
# 测试只是单个超过与只是总和超过的情况
from pymongo import MongoClient
from unittest import TestCase


class TestAggregateSizeLimit(TestCase):

    def setUp(self):
        self.client = MongoClient()
        self.coll = self.client['test-database']['test-collection']

        with open('10mb.txt', 'r') as f:
            content = f.read()

        self.coll.insert_one({
            'filename': 1,
            'content': content
        })
        self.coll.insert_one({
            'filename': 2,
            'content': content
        })

    def tearDown(self):
        self.client.drop_database('test-database')

    def test_two_aggregate_result(self):
        result = list(self.coll.aggregate(
            [
                {'$sort': {'_id': 1}},
                {'$group': {'_id': '$filename', 'content': {'$first': '$content'}}}
            ]
        ))

        if result:
            print('多个文件总和超过 16 MB，但是单个文件没有超过 16MB，没有问题')
        else:
            print('多个文件总和超过 16 MB，但是单个文件没有超过 16MB，有问题')
