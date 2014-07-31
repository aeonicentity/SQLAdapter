import sqlite3 as lite
import pickle
import time

class Log:
	def log(self,tag, message):
		self.l.write(time.asctime(time.gmtime())+" - "+tag + ": "+message+"\n")
		self.l.flush()
		return
	def __init__(self):
		self.l = open('sqlLog.log','a')
	def __del__(self):
		self.l.close()
		

class SQL:
	def initalizeLog(self):
		self.log = Log()
		return
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
			self.c.execute(query)
			
		except lite.Error ,e:
			return False
		self.con.commit()
		return True
	
	def update(self,table,data,args = None):
		print 'Updating '+table
		
		tableNameQuery = "PRAGMA table_info("+table+")"
		cols = {}
		for row in self.c.execute(tableNameQuery):
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
			self.c.execute(query)
		except lite.Error ,e:
			print "error: "+str(e)
			return False
		self.con.commit()
		return True
		
	def insert(self,table,data):
		print "Inserting into " +table
		tableNameQuery = "PRAGMA table_info("+table+")"
		cols = {}
		for row in self.c.execute(tableNameQuery):
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
		
		#self.log.log("Insert",query)
		try:
			self.c.execute(query)
			self.con.commit()
			return self.c.lastrowid
		except lite.Error, e:
			print e
			return False
		
	def select(self,table,id):
		print "Fetching from "+table
		tableNameQuery = "PRAGMA table_info("+table+")"
		keys=[]
		for row in self.c.execute(tableNameQuery):
			keys.append( row[1] )
		
		query = "SELECT * FROM `"+table+"` WHERE id = "+str(id)+";"
		self.log.log("Select",query)
		for row in self.c.execute(query):
			temp = dict(zip(keys,row ))
			for k,v in temp.iteritems():
				if v == None:
					temp[k] = ''
			return temp 
		
	def selectAll(self,table):
		print "Fetching all from "+table
		query = "SELECT * FROM `"+table+"`;"
		#self.log.log("Select",query)
		print query
		return self.c.execute(query)
		
	def search(self,table,args):
		###args should be restructured as so [('val','operator','target')] ###
		print "Searching "+table
		ids = []
		query = "SELECT `id` FROM `"+table+"` WHERE "
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
		
		#self.log.log("Search",query)
		for row in self.c.execute(query):
			ids.append(row[0])
		return ids
	def initializeDB(self):
		return
	def __init__(self):
	###TODO: connect to the sql or sqlite server###
		try:
			dbname = 'amrs.db'
			print "connecting to "+dbname+"..."
			self.con = lite.connect(dbname)
			print "connected."
			self.c = self.con.cursor()
			self.initializeDB()
		except lite.Error,e:
			print "connection failed: "+str(e)
		self.initalizeLog()
		return
	def __del__(self):
		self.con.commit()
		self.con.close()
