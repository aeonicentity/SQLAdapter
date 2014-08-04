__author__ = 'aeon'

import mysql.connector

class MySqlAdapter:
    equal = "="
    gt = ">"
    gte = ">="
    lt = "<"
    lte = "<="
    like = "LIKE"

    def delete(self,table,args):
        print "Deleting From "+table
        query = "DELETE FROM "+table+" WHERE "
        i = 0
        for term in args:
            query += "`"+ str(term[0]) +"` "+ str(term[1]) +" '"+ str(term[2]) +"'"
            if len(args)>1 and i<len(args)-1:
                query += " AND "
            i+=1
        query += ";"
        self.log.log("Delete",query)
        try:
            self.query(query)

        except mysql.connector.Error ,e:
            return False
        return True

    def update(self,table,data,args = None):
        print 'Updating '+table

        tableColumnQuery = "DESCRIBE "+table+""
        cols = {}
        for row in self.query(tableColumnQuery):
            if row[1] in data:
                cols[row[1]] = str( data[row[1]] )

        query = "UPDATE "+table+" SET "
        for k,v in cols.iteritems():
            query += k+' = "'+v+'",'

        query = query[:len(query)-1]+" "
        if args:
            query += "WHERE "
            i=0

            for term in args:
                query += "`"+ str(term[0]) +"` "+ str(term[1]) +" '"+ str(term[2]) +"'"
                if len(args)>1 and i<len(args)-1:
                    query += " AND "
                i+=1

        query += ";"
        #print query
        self.log.log("Update",query)
        try:
            self.query(query)
        except mysql.connector.Error ,e:
            print "error: "+str(e)
            return False
        return True

    def insert(self,table,data):
        print "Inserting into " +table
        tableNameQuery = "PRAGMA table_info("+table+")"
        cols = {}
        for row in self.query(tableNameQuery):
            if row[1] in data: # if the column is in the data array, set the value
                cols[row[1]] = data[row[1]]

        query = "INSERT INTO "+table+" "
        columnNames = "("
        values = "VALUES ("
        for k,v in cols.iteritems():
            columnNames += "`"+k+"`,"
            values += '"'+str(v)+'",'

        columnNames = columnNames[:len(columnNames)-1] +" "
        columnNames += ") "
        values = values[:len(values)-1] +" "
        values += ");"
        query += columnNames + values

        try:
            self.query(query)
            self.con.commit()
            return self.cursor.lastrowid
        except mysql.connector.Error, e:
            print e
            return False

    def selectId(self,table,id):
        #this function only selects the row with the correct index id. I t
        print "Fetching from "+table
        target = self.query("SHOW INDEX FROM "+table)[0][4]
        tableColumnQuery = "DESCRIBE "+table+""
        keys=[]
        for row in self.query(tableColumnQuery):
            keys.append( row[1] )

        query = "SELECT * FROM `"+table+"` WHERE `"+target+"` = "+str(id)+";"
        for row in self.query(query):
            temp = dict(zip(keys,row ))
            for k,v in temp.iteritems():
                if v == None:
                    temp[k] = ''
            return temp

    def searchAndSelect(self,table,args):
        #this function searches the database for all ids matching description, then selects them.
        results = []
        ids = self.search(table,args)
        for id in ids:
            results.append(self.selectId(table,id))
        return results

    def selectAll(self,table):
        print "Fetching all from "+table
        query = "SELECT * FROM `"+table+"`;"
        print query
        return self.query(query)

    def search(self,table,args):
        ###args should be restructured as so [('val','operator','target')] ###
        target = self.query("SHOW INDEX FROM "+table)[0][4]
        print "Searching "+table
        ids = []
        query = "SELECT `"+target+"` FROM `"+table+"` WHERE "
        i = 0
        for term in args:
            if term[1] == 'like':
                wild = '%'
            else: wild = ''
            query += "`"+ str(term[0]) +"` "+ str(term[1]) + " '"+wild + str(term[2]) + wild+"'"
            if i>=0 and i < len(args)-1:
                query += " AND "
            i += 1
        i = 0
        print query
        for row in self.query(query):
            ids.append(row[0])
        return ids

    def query(self, q):
        self.cursor.execute(q)
        try:
            rows = self.cursor.fetchall()
            return rows
        except mysql.connector.InterfaceError:
            return None

    def __init__(self, url, user, passwd, database):
        config = {
            'user': user,
            'password': passwd,
            'host': url,
            'database': database,
        }

        self.cnx = mysql.connector.connect(**config)
        self.cursor = self.cnx.cursor()
        print "connected to "+url
        return

    def __del__(self):
        self.cursor.close()
        self.cnx.close()
        return

foo = MySqlAdapter("www.novak-adapt.com","novakada",";lkjpoi09","novakada_sales")
id = foo.search("salesmen",[("fName","=","Eric")])
print foo.selectId("salesmen",id[0])