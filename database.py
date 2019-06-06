import sqlite3
import os
import sys

class UFCHistoryDB:
	""" manages sqlite database
	"""

	def __init__(self, db_file):
		""" constructor """

		self.db_file_ = db_file

		self.delete_database()
		
		self.conn = self.create_connection(db_file)

		if self.conn == None:
			print("Cannot create connection! ")
			exit()

		self.c = self.conn.cursor()

		self.create_tables()

	def create_connection(self, db_file):
		"""create a database connection to the SQLite database
			specified by db_file
		:param db_file: database file
		:return: Connection object or None
		"""

		try:
			conn = sqlite3.connect(db_file)
			return conn
		except Error as e:
			print(e)
	 
		return None

	def close_connection(self):
		"""close a database connection to the SQLite database
		:param:
		:return: 
		"""

		try:
			self.c.close()
		except Exception as e:
			raise e
		
		try:
			self.conn.close()
		except Exception as e:
			raise e

	def create_tables(self):
		""" create necessary tables 
		:param 
		:return: 
		"""

		self.c.execute("""CREATE TABLE IF NOT EXISTS Fighters (
					id integer PRIMARY KEY,
					name text NOT NULL,
					age integer,
					url text NOT NULL,
					height text,
					weight text,
					weight_class text,
					reach text,
					group_name text
					)""")

		self.c.execute("""CREATE TABLE IF NOT EXISTS History (
					id integer NOT NULL,
					match_date text NOT NULL,
					event text NOT NULL,
					opponent text NOT NULL,
					opp_url text,
					result text NOT NULL,
					decision text NOT NULL,
					rnd integer NOT NULL,
					match_time text NOT NULL
					)""")

		self.c.execute("""CREATE TABLE IF NOT EXISTS StandingStatistics (
					id integer NOT NULL,
					match_date text NOT NULL,
					opponent text NOT NULL,
					opp_url text NOT NULL,
					sdbl_a text,
					sdhl_a text,
					sdll_a text,
					tsl text,
					tsa text,
					ssl text,
					ssa text,
					sa text,
					kd text,
					percent_body text,
					percent_head text,
					percent_leg text
					)""")

		self.c.execute("""CREATE TABLE IF NOT EXISTS ClinchStatistics (
					id integer NOT NULL,
					match_date text NOT NULL,
					opponent text NOT NULL,
					opp_url text NOT NULL,
					scbl text,
					scba text,
					schl text,
					scha integer,
					scll text,
					scla text,
					rv text,
					sr text,
					tdl text,
					tda text,
					tds text,
					td_percent text
					)""")

		self.c.execute("""CREATE TABLE IF NOT EXISTS GroundStatistics (
					id integer NOT NULL,
					match_date text NOT NULL,
					opponent text NOT NULL,
					opp_url text NOT NULL,
					sgbl text,
					sgba text,
					sghl text,
					sgha text,
					sgll text,
					sgla text,
					ad text,
					adtb text,
					adhg text,
					adtm text,
					adts text,
					sm text
					)""")

		self.conn.commit()
		self.reconnect_database(self.db_file_)

	def delete_database(self):
		""" delete database db_name
		:param
		:return: true on success, false on fail
		"""

		try:
			os.remove(self.db_file_)
		except Exception as e:
			pass
		

	def reconnect_database(self, db_file):
		""" close the connection to database db_name and reconnect to db_name
		:param db_file: database file
		:return:
		"""

		self.c.close()
		self.conn.close()

		# reconnect to database
		try:
			self.conn = sqlite3.connect(db_file)
		except Exception as e:
			raise e

		self.c = self.conn.cursor()

	def insert_into_table_fighters(self, id_, data):
		""" insert given 'data' into table 'Fighters'

		:param id_: unique fighter identifier
		:param data: dictionary of fighter general information
		:return:
		"""

		sql = """INSERT INTO Fighters (id, name, age, url, height, weight, weight_class, reach, group_name) 
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
		val = None

		try:
			val = (id_, data['name'], data['age'], data['url'], data['height'], data['weight']
				, data['weight_class'], data['reach'], data['group_name'])
		except Exception as e:
			# print("Error(DB.Fighters): ", e)
			pass

		if val != None:
			try:
				self.c.execute(sql, val)
				self.conn.commit()
			except Exception as e:
				print("Error while inserting into table 'Fighters':", e)
				print("Query : ", sql, val)
		
	def insert_into_table_history(self, id_, data):
		""" insert given 'data' into table 'History'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		for item in data:
			sql = """INSERT INTO History (id, match_date, event, opponent, opp_url, result, decision, rnd, match_time) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				val = (id_, item['DATE'], item['EVENT'], item['OPPONENT'], item['opp_url'], item['RESULT']
					, item['DECISION'], item['RND'], item['TIME'])
			except Exception as e:
				print("Error(DB.History): ", e)

			if val != None:
				try:
					self.c.execute(sql, val)
					self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'History':", e)
					print("Query : ", sql, val)

	def insert_into_table_standing_stats(self, id_, data):
		""" insert given 'data' into table 'StandingStatistics'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		for item in data:
			sql = """INSERT INTO StandingStatistics (id, match_date, opponent, opp_url, sdbl_a, sdhl_a, sdll_a, tsl, 
													tsa, ssl, sa, kd, percent_body, percent_head, percent_leg) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				val = (id_, item['DATE'], item['OPP'], item['opp_url'], item['SDBL/A'], item['SDHL/A'], item['SDLL/A']
					, item['TSL'], item['TSA'], item['SSL'], item['SA'], item['KD'], item['PERCENTBODY'], item['PERCENTHEAD'], item['PERCENTLEG'])
			except Exception as e:
				print("Error(DB.StandingStatistics): ", e)

			if val != None:
				try:
					self.c.execute(sql, val)
					self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'StandingStatistics':", e)
					print("Query : ", sql, val)

	def insert_into_table_clinch_stats(self, id_, data):
		""" insert given 'data' into table 'ClinchStatistics'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		for item in data:
			sql = """INSERT INTO ClinchStatistics (id, match_date, opponent, opp_url, scbl, scba, schl, scha, scll, 
													scla, rv, sr, tdl, tda, tds, td_percent) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				val = (id_, item['DATE'], item['OPP'], item['opp_url'], item['SCBL'], item['SCBA'], item['SCHL']
					, item['SCHA'], item['SCLL'], item['SCLA'], item['RV'], item['SR'], item['TDL'], item['TDA'], item['TDS'], item['TDPERCENT'])
			except Exception as e:
				print("Error(DB.ClinchStatistics): ", e)

			if val != None:
				try:
					self.c.execute(sql, val)
					self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'ClinchStatistics':", e)
					print("Query : ", sql, val)

	def insert_into_table_ground_stats(self, id_, data):
		""" insert given 'data' into table 'GroundStatistics'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		for item in data:
			sql = """INSERT INTO GroundStatistics (id, match_date, opponent, opp_url, sgbl, sgba, sghl, sgha, sgll, 
													sgla, ad, adtb, adhg, adtm, adts, sm) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				val = (id_, item['DATE'], item['OPP'], item['opp_url'], item['SGBL'], item['SGBA'], item['SGHL']
					, item['SGHA'], item['SGLL'], item['SGLA'], item['AD'], item['ADTB'], item['ADHG'], item['ADTM'], item['ADTS'], item['SM'])
			except Exception as e:
				print("Error(DB.GroundStatistics): ", e)

			if val != None:
				try:
					self.c.execute(sql, val)
					self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'GroundStatistics':", e)
					print("Query : ", sql, val)

	def get_rows_for_schema(self):
		""" get all rows to be written onto the excel file
		param:
		return:
		"""

		

if __name__ == "__main__":

	print("Please run the main script!")