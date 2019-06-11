import sqlite3
import os
import sys
import progressbar

class UFCHistoryDB:
	""" manages sqlite database
	"""

	def __init__(self, db_file, delete_if_exists = False, sub_folder = None):
		""" constructor 
		param db_file: database file name
		param delete_if_exists: True: delete 'db_file' if it already exists, False: do nothing
		param sub_folder: create a subdirectory 'sub_folder' and create database file in it
		return:
		"""

		self.path_ = ''

		if sub_folder != None:
			if not os.path.exists(sub_folder):
				os.makedirs(sub_folder)
			dir_path = os.path.dirname(os.path.realpath(__file__))
			self.db_file_ = f'{dir_path}\\{sub_folder}\\{db_file}'
		else:
			self.db_file_ = os.path.join(os.path.dirname(os.path.realpath(__file__)), db_file)

		# is_db_file_deleted = self.delete_database()
		if delete_if_exists and os.path.isfile(self.db_file_):
			if self.delete_database() == False:
				exit()

		self.conn = self.create_connection(self.db_file_)

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
		except Exception as e:
			print(str(e))
	 
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
					opp_url text,
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
					opp_url text,
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
					opp_url text,
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
		self.reconnect_database()

	def delete_database(self):
		""" delete database db_name
		:param
		:return: true on success, false on fail
		"""

		try:
			os.remove(self.db_file_)
			return True
		except Exception as e:
			print(f'Error(DB.delete_database): {str(e)}')
			return False
		

	def reconnect_database(self):
		""" close the connection to database db_name and reconnect to db_name
		:param db_file: database file
		:return:
		"""

		self.c.close()
		self.conn.close()

		# reconnect to database
		try:
			self.conn = sqlite3.connect(self.db_file_)
		except Exception as e:
			raise e

		self.c = self.conn.cursor()

	def execute(self, query):
		""" execute query
		param query: query
		"""

		self.c.execute(query)

	def insert_into_table_fighters(self, id_, data):
		""" insert given 'data' into table 'Fighters'

		:param id_: unique fighter identifier
		:param data: dictionary of fighter general information
		:return:
		"""

		if data is None:
			print('Fighter data is None!')
			return

		sql = """INSERT INTO Fighters (id, name, age, url, height, weight, weight_class, reach, group_name) 
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
		val = None

		try:
			val = (id_, data['name'], data['age'], data['url'], data['height'], data['weight']
				, data['weight_class'], data['reach'], data['group_name'])
		except Exception as e:
			print("Error(DB.Fighters): ", str(e))
			# pass

		if val != None:
			try:
				self.c.execute(sql, val)
				# self.conn.commit()
			except Exception as e:
				print("Error while inserting into table 'Fighters':", str(e))
				print("Query : ", sql, val)
		
	def insert_into_table_history(self, id_, data):
		""" insert given 'data' into table 'History'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		if data is None:
			return

		for item in data:
			sql = """INSERT INTO History (id, match_date, event, opponent, opp_url, result, decision, rnd, match_time) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				if 'opp_url' in item:
					val = (id_, item['DATE'], item['EVENT'], item['OPPONENT'], item['opp_url'], item['RESULT']
						, item['DECISION'], item['RND'], item['TIME'])
				else:
					val = (id_, item['DATE'], item['EVENT'], item['OPPONENT'], None, item['RESULT']
						, item['DECISION'], item['RND'], item['TIME'])
			except Exception as e:
				print("Error(DB.History): ", str(e))

			if val != None:
				try:
					self.c.execute(sql, val)
					# self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'History':", str(e))
					print("Query : ", sql, val)

	def insert_into_table_standing_stats(self, id_, data):
		""" insert given 'data' into table 'StandingStatistics'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		if data is None:
			return

		for item in data:
			sql = """INSERT INTO StandingStatistics (id, match_date, opponent, opp_url, sdbl_a, sdhl_a, sdll_a, tsl, 
													tsa, ssl, sa, kd, percent_body, percent_head, percent_leg) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				if 'opp_url' in item:
					val = (id_, item['DATE'], item['OPP'], item['opp_url'], item['SDBL/A'], item['SDHL/A'], item['SDLL/A']
						, item['TSL'], item['TSA'], item['SSL'], item['SA'], item['KD'], item['PERCENTBODY'], item['PERCENTHEAD'], item['PERCENTLEG'])
				else:
					val = (id_, item['DATE'], item['OPP'], None, item['SDBL/A'], item['SDHL/A'], item['SDLL/A']
						, item['TSL'], item['TSA'], item['SSL'], item['SA'], item['KD'], item['PERCENTBODY'], item['PERCENTHEAD'], item['PERCENTLEG'])
			except Exception as e:
				print("Error(DB.StandingStatistics): ", str(e))

			if val != None:
				try:
					self.c.execute(sql, val)
					# self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'StandingStatistics':", str(e))
					print("Query : ", sql, val)

	def insert_into_table_clinch_stats(self, id_, data):
		""" insert given 'data' into table 'ClinchStatistics'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		if data is None:
			return

		for item in data:
			sql = """INSERT INTO ClinchStatistics (id, match_date, opponent, opp_url, scbl, scba, schl, scha, scll, 
													scla, rv, sr, tdl, tda, tds, td_percent) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				if 'opp_url' in item:
					val = (id_, item['DATE'], item['OPP'], item['opp_url'], item['SCBL'], item['SCBA'], item['SCHL']
						, item['SCHA'], item['SCLL'], item['SCLA'], item['RV'], item['SR'], item['TDL'], item['TDA'], item['TDS'], item['TDPERCENT'])
				else:
					val = (id_, item['DATE'], item['OPP'], None, item['SCBL'], item['SCBA'], item['SCHL']
						, item['SCHA'], item['SCLL'], item['SCLA'], item['RV'], item['SR'], item['TDL'], item['TDA'], item['TDS'], item['TDPERCENT'])
			except Exception as e:
				print("Error(DB.ClinchStatistics): ", str(e))

			if val != None:
				try:
					self.c.execute(sql, val)
					# self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'ClinchStatistics':", str(e))
					print("Query : ", sql, val)

	def insert_into_table_ground_stats(self, id_, data):
		""" insert given 'data' into table 'GroundStatistics'
		:param id_: unique fighter identifier
		:param data: list of sub lists
		:return:
		"""

		if data is None:
			return

		for item in data:
			sql = """INSERT INTO GroundStatistics (id, match_date, opponent, opp_url, sgbl, sgba, sghl, sgha, sgll, 
													sgla, ad, adtb, adhg, adtm, adts, sm) 
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
			val = None

			try:
				if 'opp_url' in item:
					val = (id_, item['DATE'], item['OPP'], item['opp_url'], item['SGBL'], item['SGBA'], item['SGHL']
						, item['SGHA'], item['SGLL'], item['SGLA'], item['AD'], item['ADTB'], item['ADHG'], item['ADTM'], item['ADTS'], item['SM'])
				else:
					val = (id_, item['DATE'], item['OPP'], None, item['SGBL'], item['SGBA'], item['SGHL']
						, item['SGHA'], item['SGLL'], item['SGLA'], item['AD'], item['ADTB'], item['ADHG'], item['ADTM'], item['ADTS'], item['SM'])
			except Exception as e:
				print("Error(DB.GroundStatistics): ", str(e))

			if val != None:
				try:
					self.c.execute(sql, val)
					# self.conn.commit()
				except Exception as e:
					print("Error while inserting into table 'GroundStatistics':", str(e))
					print("Query : ", sql, val)

	def get_rows_for_schema(self):
		""" get all rows to be written onto the excel file
		param:
		return: list of rows
		"""

		# make a query to get initial data
		sql = """SELECT History.match_date, Fighters.weight_class, History.decision, History.rnd, History.match_time, 
						History.event, Fighters.id, Fighters.name, Fighters.height, Fighters.reach, Fighters.age, Fighters.url, 
						History.opponent, History.result, History.opp_url
						FROM Fighters, History WHERE Fighters.id == History.id
						ORDER BY match_date ASC"""

		# list of dictionaries, each dictionary contains a match information fit for schema
		# NOTE: Using dictionary rather than list makes it easier to change/revise and maintain
		# can easily understand what is what
		match_list = []

		rows = self.c.execute(sql).fetchall()

		get_rows_bar = progressbar.ProgressBar(maxval=len(rows), \
									widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(rows))])
		get_rows_bar.start()

		print('Getting rows for excel output from database...')

		for index, row in enumerate(rows):
			# print(row)
			# dictionary to contain match information
			dictionary = {}

			# General Info (From Fight History Page)
			dictionary['Date'] = row[0]
			dictionary['WeightClass'] = row[1]

			if row[13] == 'Win':
				dictionary['Winner'] = row[7]
			elif row[13] == 'Loss':
				dictionary['Winner'] = row[12]
			else:
				dictionary['Winner'] = ''

			dictionary['DecisionType'] = row[2]
			dictionary['Rounds'] = row[3]
			dictionary['Time'] = row[4]
			dictionary['IsTitle?'] = row[5]

			# F1 Fighter Info

			dictionary['F1Id'] = row[6]
			dictionary['F1Name'] = row[7]
			dictionary['F1Height'] = row[8]
			dictionary['F1Reach'] = row[9]
			dictionary['F1Age'] = row[10]

			# F1 Striking Stats

			sql = """ SELECT sdbl_a, sdhl_a, sdll_a, tsl, tsa, ssl, ssa, sa, kd 
								FROM StandingStatistics WHERE id=? AND match_date=?
			"""

			val = (row[6], row[0])

			try:
				sql_result = self.c.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F1SDBL'] = sql_result[0].split('/')[0] if '/' in sql_result[0] else ''
					dictionary['F1SDBA'] = sql_result[0].split('/')[1] if '/' in sql_result[0] else ''
					dictionary['F1SDHL'] = sql_result[1].split('/')[0] if '/' in sql_result[1] else ''
					dictionary['F1SDHA'] = sql_result[1].split('/')[1] if '/' in sql_result[1] else ''
					dictionary['F1SDLL'] = sql_result[2].split('/')[0] if '/' in sql_result[2] else ''
					dictionary['F1SDLA'] = sql_result[2].split('/')[1] if '/' in sql_result[2] else ''
					dictionary['F1TSL'] = sql_result[3]
					dictionary['F1TSA'] = sql_result[4]
					dictionary['F1SSL'] = sql_result[5]
					dictionary['F1SSA'] = sql_result[6]
					dictionary['F1SA'] = sql_result[7]
					dictionary['F1KD'] = sql_result[8]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get StandingStatistics information due to above error.")

			# F1 Clinch Stats

			sql = """ SELECT scbl, scba, schl, scha, scll, scla, rv, sr, tdl, tda, tds 
								FROM ClinchStatistics WHERE id=? AND match_date=?
			"""

			val = (row[6], row[0])

			try:
				sql_result = self.c.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F1SCBL'] = sql_result[0]
					dictionary['F1SCBA'] = sql_result[1]
					dictionary['F1SCHL'] = sql_result[2]
					dictionary['F1SCHA'] = sql_result[3]
					dictionary['F1SCLL'] = sql_result[4]
					dictionary['F1SCLA'] = sql_result[5]
					dictionary['F1RV'] = sql_result[6]
					dictionary['F1SR'] = sql_result[7]
					dictionary['F1TDL'] = sql_result[8]
					dictionary['F1TDA'] = sql_result[9]
					dictionary['F1TDS'] = sql_result[10]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get ClinchStatistics information due to above error.")

			# F1 Ground Stats

			sql_gs = """ SELECT sgbl, sgba, sghl, sgha, sgll, sgla, ad, adtb, adhg, adtm, adts, sm 
								FROM GroundStatistics WHERE id=? AND match_date=?
			"""

			val = (row[6], row[0])

			try:
				sql_result = self.c.execute(sql_gs, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F1SGBL'] = sql_result[0]
					dictionary['F1SGBA'] = sql_result[1]
					dictionary['F1SGHL'] = sql_result[2]
					dictionary['F1SGHA'] = sql_result[3]
					dictionary['F1SGLL'] = sql_result[4]
					dictionary['F1SGLA'] = sql_result[5]
					dictionary['F1AD'] = sql_result[6]
					dictionary['F1ADTB'] = sql_result[7]
					dictionary['F1ADHG'] = sql_result[8]
					dictionary['F1ADTM'] = sql_result[9]
					dictionary['F1ADTS'] = sql_result[10]
					dictionary['F1SM'] = sql_result[11]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get GroundStatistics information due to above error.")

			# F2 Fighter Info
			
			sql = """SELECT id, name, height, reach, age
							FROM Fighters WHERE name=? and url=?
			"""

			val = (row[12], row[14])
			# print(val)

			try:
				sql_result = self.c.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F2Id'] = sql_result[0]
					dictionary['F2Name'] = sql_result[1]
					dictionary['F2Height'] = sql_result[2]
					dictionary['F2Reach'] = sql_result[3]
					dictionary['F2Age'] = sql_result[4]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get fighter 2 information due to above error.")

			if 'F2Id' not in dictionary: # skip over if identifier of fighter 2 is not avaiable
				index += 1
				get_rows_bar.update(index)
				continue

			# F2 Striking Stats

			sql = """ SELECT sdbl_a, sdhl_a, sdll_a, tsl, tsa, ssl, ssa, sa, kd 
								FROM StandingStatistics WHERE id=? AND match_date=?
			"""

			val = (dictionary['F2Id'], row[0])

			try:
				sql_result = self.c.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F2SDBL'] = sql_result[0].split('/')[0] if '/' in sql_result[0] else ''
					dictionary['F2SDBA'] = sql_result[0].split('/')[1] if '/' in sql_result[0] else ''
					dictionary['F2SDHL'] = sql_result[1].split('/')[0] if '/' in sql_result[1] else ''
					dictionary['F2SDHA'] = sql_result[1].split('/')[1] if '/' in sql_result[1] else ''
					dictionary['F2SDLL'] = sql_result[2].split('/')[0] if '/' in sql_result[2] else ''
					dictionary['F2SDLA'] = sql_result[2].split('/')[1] if '/' in sql_result[2] else ''
					dictionary['F2TSL'] = sql_result[3]
					dictionary['F2TSA'] = sql_result[4]
					dictionary['F2SSL'] = sql_result[5]
					dictionary['F2SSA'] = sql_result[6]
					dictionary['F2SA'] = sql_result[7]
					dictionary['F2KD'] = sql_result[8]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get StandingStatistics information due to above error(F2).")

			# F2 Clinch Stats

			sql = """ SELECT scbl, scba, schl, scha, scll, scla, rv, sr, tdl, tda, tds 
								FROM ClinchStatistics WHERE id=? AND match_date=?
			"""

			val = (dictionary['F2Id'], row[0])

			try:
				sql_result = self.c.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F2SCBL'] = sql_result[0]
					dictionary['F2SCBA'] = sql_result[1]
					dictionary['F2SCHL'] = sql_result[2]
					dictionary['F2SCHA'] = sql_result[3]
					dictionary['F2SCLL'] = sql_result[4]
					dictionary['F2SCLA'] = sql_result[5]
					dictionary['F2RV'] = sql_result[6]
					dictionary['F2SR'] = sql_result[7]
					dictionary['F2TDL'] = sql_result[8]
					dictionary['F2TDA'] = sql_result[9]
					dictionary['F2TDS'] = sql_result[10]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get ClinchStatistics information due to above error(F2).")

			# F2 Ground Stats

			sql = """ SELECT sgbl, sgba, sghl, sgha, sgll, sgla, ad, adtb, adhg, adtm, adts, sm 
								FROM GroundStatistics WHERE id=? AND match_date=?
			"""

			val = (dictionary['F2Id'], row[0])

			try:
				sql_result = self.c.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F2SGBL'] = sql_result[0]
					dictionary['F2SGBA'] = sql_result[1]
					dictionary['F2SGHL'] = sql_result[2]
					dictionary['F2SGHA'] = sql_result[3]
					dictionary['F2SGLL'] = sql_result[4]
					dictionary['F2SGLA'] = sql_result[5]
					dictionary['F2AD'] = sql_result[6]
					dictionary['F2ADTB'] = sql_result[7]
					dictionary['F2ADHG'] = sql_result[8]
					dictionary['F2ADTM'] = sql_result[9]
					dictionary['F2ADTS'] = sql_result[10]
					dictionary['F2SM'] = sql_result[11]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get GroundStatistics information due to above error(F2).")

			# the dictionary is filled up, let's add it into the list
			match_list.append(dictionary)

			index += 1
			
			get_rows_bar.update(index)
			# print(dictionary)
			# print()

		get_rows_bar.finish()
		return match_list


if __name__ == "__main__":

	db = UFCHistoryDB('ufc_history_01.db', True, 'tmp_data')
	db.get_rows_for_schema()

	print("Please run the main script!")