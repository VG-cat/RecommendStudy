#!/usr/bin/env python
# encoding: utf-8


"""
@author:  'Administrator'
@contact:  
@time: 
"""
import happybase
import time

class HBaseManager:

    config = {
        'BATCH_SIZE':1000
    }

    def __init__(self, host='localhost',port=9090, namespace='default', table_name=''):
        """ Connect to HBase server.
        This will use the host, namespace, table name, and batch size as defined in
        the global variables above.
        """
        self.conn = happybase.Connection(host = host,port=port,
            table_prefix = namespace,table_prefix_separator=':')
        self.conn.open()
        self.table = self.conn.table(table_name)
        self.batch = self.table.batch(batch_size = self.config['BATCH_SIZE'])

    def append_row(self,rowkey,family,col,content):
        self.table.put(rowkey, { '%s:%s' % (family,col) : content } )

    def close(self):
        self.conn.close()

    def filter_rows(self,filter=None):
        if filter:
            rs = self.table.scan(filter=filter)
            return rs
        else :
            rs = self.table.scan()
            return rs