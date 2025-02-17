#!/usr/bin/env python
# encoding: utf-8


"""
@author:  'Administrator'
@contact:  
@time: 
"""

import happybase

hostname = '192.168.10.202'
table_name = 'users'
column_family = 'cf'
row_key = 'row_1'

conn = happybase.Connection(hostname)


def pretty_print(msg):
    print msg.ljust(80, '*')

def show_tables():

    pretty_print('show all tables now')
    tables =  conn.tables()
    for t in tables:
        print t

def create_table(table_name, column_family):
    pretty_print('create table %s' % table_name)
    conn.create_table(table_name, {column_family:dict()})


def show_rows(table, row_keys=None):
    if row_keys:
        pretty_print('show value of row named %s' % row_keys)
        if len(row_keys) == 1:
            print table.row(row_keys[0])
        else:
            print table.rows(row_keys)
    else:
        pretty_print('show all row values of table named %s' % table.name)
        for key, value in table.scan():
            print key, value

def put_row(table, column_family, row_key, value):
    pretty_print('insert one row to hbase')
    table.put(row_key, {'%s:name' % column_family:'name_%s' % value})

def put_rows(table, column_family, row_lines=30):
    pretty_print('insert rows to hbase now')
    for i in range(row_lines):
        put_row(table, column_family, 'row_%s' % i, i)

def delete_row(table, row_key, column_family=None, keys=None):
    if keys:
        pretty_print('delete keys:%s from row_key:%s' % (keys, row_key))
        key_list = ['%s:%s' % (column_family, key) for key in keys]
        table.delete(row_key, key_list)
    else:
        pretty_print('delete row(column_family:) from hbase')
        table.delete(row_key)
def counter(table, row_key, column_family):
    pretty_print('test counter now.')

    print table.counter_inc(row_key, '%s:counter' % column_family)  # prints 1
    print table.counter_inc(row_key, '%s:counter' % column_family)  # prints 2
    print table.counter_inc(row_key, '%s:counter' % column_family)  # prints 3
    print table.counter_dec(row_key, '%s:counter' % column_family)  # prints 2
    print table.counter_inc(row_key, '%s:counter' % column_family, value=3)  # prints 5
    print table.counter_get(row_key, '%s:counter' % column_family)  # prints 5
    table.counter_set(row_key, '%s:counter' % column_family, 12)
    print table.counter_get(row_key, '%s:counter' % column_family)  # prints 5

def delete_table(table_name):
    pretty_print('delete table %s now.' % table_name)
    conn.delete_table(table_name, True)

def pool():
    pretty_print('test pool connection now.')
    pool = happybase.ConnectionPool(size=3, host=hostname)
    with pool.connection() as connection:
        print connection.tables()

def main():
    # show_tables()
    # create_table(table_name, column_family)
    # show_tables()

    table = conn.table(table_name)
    show_rows(table)
    put_rows(table, column_family)
    show_rows(table)
    #
    # # 更新操作
    # put_row(table, column_family, row_key, 'xiaoh.me')
    # show_rows(table, [row_key])
    #
    # # 删除数据
    # delete_row(table, row_key)
    # show_rows(table, [row_key])
    #
    # delete_row(table, row_key, column_family, ['name'])
    # show_rows(table, [row_key])
    #
    # counter(table, row_key, column_family)
    #
    # delete_table(table_name)

if __name__ == "__main__":
    main()
