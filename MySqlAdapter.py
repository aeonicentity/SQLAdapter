__author__ = 'aeon'

import mysql.connector

class MySqlAdapter:

    def query(self, q):
        self.cursor.execute(q)
        try:
            rows = self.cursor.fetchall()
            return rows
        except mysql.connector.InterfaceError:
            return None

    def __init__(self, url, user, passwd, database):
        config = {
            'user':user,
            'password':passwd,
            'host':url,
            'database':database,
        }

        self.cnx = mysql.connector.connect(**config)
        self.cursor = self.cnx.cursor()
        print "connected to "+url
        return

    def __del__(self):
        self.cursor.close()
        self.cnx.close()
        return