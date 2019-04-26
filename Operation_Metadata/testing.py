import cx_Oracle
import csv
import os as os
import proj_constant_var as const
import glob
import datetime

conn_str = cx_Oracle.connect(const.database_connection)
cursor = conn_str.cursor()
#var = cursor.var(cx_Oracle.OBJECT, typename = "TEST_TYPE")
#cursor.setinputsizes(None, var)
cursor.callproc("testing", [ None])