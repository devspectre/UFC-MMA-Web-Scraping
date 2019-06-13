
import sqlite3
import os
import sys
import threading
import progressbar
from shutil import copyfile
from shutil import rmtree
from excel import ExcelWriter

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

		if delete_if_exists:
			self.create_tables()

		self.c.execute("PRAGMA journal_mode = OFF")
		
		self.conn.commit()

		# used to preserve all queried data which should be written to excel
		self.rows_for_schema = []

		# progress bar for above list processing
		self.get_rows_bar = None

		# used to check if all threads are finished
		self.total_row_count = 0

		# count of threads
		self.thread_count = 20

		# path to temp dir for multithreading
		self.tmp_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'temp')

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

		self.c.execute("""CREATE INDEX index_name ON Fighters(name)
			""")

		self.c.execute("""CREATE UNIQUE INDEX index_url ON Fighters(url)
			""")

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

		if data is None or len(data) == 0:
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

	def write_to_excel(self, rows):
		""" writes rows to excel
		param rows: a list of dictionaries
		return: number of rows written successfully
		"""

		# create an excel writer instance
		xw = ExcelWriter('ufc_history')
		
		# create horizontal header
		xw.set_header_list(xw.header_list)

		# initialize variables row_index, our default template starts from row 4
		row_index = 4

		xw_bar = progressbar.ProgressBar(maxval=len(rows), \
										widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(rows))])
		xw_bar.start()

		print('Writing to excel...')

		# write to excel file
		# NOTE: Using dictionary rather than list makes it easier to change/revise and maintain
		# can easily understand what is what
		for row in rows:
			if row is None or len(row) == 0:
				continue
			try:
				xw.write_to_sheet(row_index, 0, row['Date'])
				xw.write_to_sheet(row_index, 1, row['WeightClass'])
				xw.write_to_sheet(row_index, 2, row['Winner'])
				xw.write_to_sheet(row_index, 3, row['DecisionType'])
				xw.write_to_sheet(row_index, 4, row['Rounds'])
				xw.write_to_sheet(row_index, 5, row['Time'])
				xw.write_to_sheet(row_index, 6, row['IsTitle?'])

				if 'F1Name' in row:
					xw.write_to_sheet(row_index, 7, row['F1Name'])
					xw.write_to_sheet(row_index, 8, row['F1Height'])
					xw.write_to_sheet(row_index, 9, row['F1Reach'])
					xw.write_to_sheet(row_index, 10, row['F1Age'])

				if 'F1SDBL' in row:
					xw.write_to_sheet(row_index, 11, row['F1SDBL'])
					xw.write_to_sheet(row_index, 12, row['F1SDBA'])
					xw.write_to_sheet(row_index, 13, row['F1SDHL'])
					xw.write_to_sheet(row_index, 14, row['F1SDHA'])
					xw.write_to_sheet(row_index, 15, row['F1SDLL'])
					xw.write_to_sheet(row_index, 16, row['F1SDLA'])
					xw.write_to_sheet(row_index, 17, row['F1TSL'])
					xw.write_to_sheet(row_index, 18, row['F1TSA'])
					xw.write_to_sheet(row_index, 19, row['F1SSL'])
					xw.write_to_sheet(row_index, 20, row['F1SSA'])
					xw.write_to_sheet(row_index, 21, row['F1SA'])
					xw.write_to_sheet(row_index, 22, row['F1KD'])

					xw.write_to_sheet(row_index, 23, row['F1SCBL'])
					xw.write_to_sheet(row_index, 24, row['F1SCBA'])
					xw.write_to_sheet(row_index, 25, row['F1SCHL'])
					xw.write_to_sheet(row_index, 26, row['F1SCHA'])
					xw.write_to_sheet(row_index, 27, row['F1SCLL'])
					xw.write_to_sheet(row_index, 28, row['F1SCLA'])
					xw.write_to_sheet(row_index, 29, row['F1RV'])
					xw.write_to_sheet(row_index, 30, row['F1SR'])
					xw.write_to_sheet(row_index, 31, row['F1TDL'])
					xw.write_to_sheet(row_index, 32, row['F1TDA'])
					xw.write_to_sheet(row_index, 33, row['F1TDS'])

					xw.write_to_sheet(row_index, 34, row['F1SGBL'])
					xw.write_to_sheet(row_index, 35, row['F1SGBA'])
					xw.write_to_sheet(row_index, 36, row['F1SGHL'])
					xw.write_to_sheet(row_index, 37, row['F1SGHA'])
					xw.write_to_sheet(row_index, 38, row['F1SGLL'])
					xw.write_to_sheet(row_index, 39, row['F1SGLA'])
					xw.write_to_sheet(row_index, 40, row['F1AD'])
					xw.write_to_sheet(row_index, 41, row['F1ADTB'])
					xw.write_to_sheet(row_index, 42, row['F1ADHG'])
					xw.write_to_sheet(row_index, 43, row['F1ADTM'])
					xw.write_to_sheet(row_index, 44, row['F1ADTS'])
					xw.write_to_sheet(row_index, 45, row['F1SM'])

				if 'F2Name' in row:
					xw.write_to_sheet(row_index, 46, row['F2Name'])
					xw.write_to_sheet(row_index, 47, row['F2Height'])
					xw.write_to_sheet(row_index, 48, row['F2Reach'])
					xw.write_to_sheet(row_index, 49, row['F2Age'])

				if 'F2SDBL' in row:
					xw.write_to_sheet(row_index, 50, row['F2SDBL'])
					xw.write_to_sheet(row_index, 51, row['F2SDBA'])
					xw.write_to_sheet(row_index, 52, row['F2SDHL'])
					xw.write_to_sheet(row_index, 53, row['F2SDHA'])
					xw.write_to_sheet(row_index, 54, row['F2SDLL'])
					xw.write_to_sheet(row_index, 55, row['F2SDLA'])
					xw.write_to_sheet(row_index, 56, row['F2TSL'])
					xw.write_to_sheet(row_index, 57, row['F2TSA'])
					xw.write_to_sheet(row_index, 58, row['F2SSL'])
					xw.write_to_sheet(row_index, 59, row['F2SSA'])
					xw.write_to_sheet(row_index, 60, row['F2SA'])
					xw.write_to_sheet(row_index, 61, row['F2KD'])

					xw.write_to_sheet(row_index, 62, row['F2SCBL'])
					xw.write_to_sheet(row_index, 63, row['F2SCBA'])
					xw.write_to_sheet(row_index, 64, row['F2SCHL'])
					xw.write_to_sheet(row_index, 65, row['F2SCHA'])
					xw.write_to_sheet(row_index, 66, row['F2SCLL'])
					xw.write_to_sheet(row_index, 67, row['F2SCLA'])
					xw.write_to_sheet(row_index, 68, row['F2RV'])
					xw.write_to_sheet(row_index, 69, row['F2SR'])
					xw.write_to_sheet(row_index, 70, row['F2TDL'])
					xw.write_to_sheet(row_index, 71, row['F2TDA'])
					xw.write_to_sheet(row_index, 72, row['F2TDS'])

					xw.write_to_sheet(row_index, 73, row['F2SGBL'])
					xw.write_to_sheet(row_index, 74, row['F2SGBA'])
					xw.write_to_sheet(row_index, 75, row['F2SGHL'])
					xw.write_to_sheet(row_index, 76, row['F2SGHA'])
					xw.write_to_sheet(row_index, 77, row['F2SGLL'])
					xw.write_to_sheet(row_index, 78, row['F2SGLA'])
					xw.write_to_sheet(row_index, 79, row['F2AD'])
					xw.write_to_sheet(row_index, 80, row['F2ADTB'])
					xw.write_to_sheet(row_index, 81, row['F2ADHG'])
					xw.write_to_sheet(row_index, 82, row['F2ADTM'])
					xw.write_to_sheet(row_index, 83, row['F2ADTS'])
					xw.write_to_sheet(row_index, 84, row['F2SM'])
			except Exception as e:
				print(f'Failed to write to excel due to error: {str(e)}')
			finally:
				row_index += 1

				xw_bar.update(row_index - 4)

		# save the file and close safely
		xw.done()

		xw_bar.finish()

		print('Writing to excel is completed!')

	@staticmethod
	def atoi(a):
		""" convert string to int
		param a: source string
		return: return converted integer if possible, return 0 if string is empty or any exception
		"""

		if isinstance(a, int):
			return a

		if a is None or len(a) == 0:
			return 0

		result = 0
		try:
			result = int(a)
		except Exception as e:
			return 0

		return result

	def get_rows(self, index, rows, db_file):
		""" get rows within id_list
		param index: thread index
		param rows: list of match info and fighter 1's info
		param db_file: absolute path to the database file
		return: 
		"""

		try:
			conn_ = sqlite3.connect(db_file)

			cursor = conn_.cursor()
		except Exception as e:
			print(f'Thread({index}): Cannot connect to database {db_file}')
			return
		

		for row in rows:

			if row is None or len(row) == 0:
				continue

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

			# Fighter 1 General Information

			dictionary['F1Id'] = row[6]
			dictionary['F1Name'] = row[7].strip()
			dictionary['F1Height'] = row[8]
			dictionary['F1Reach'] = row[9]
			dictionary['F1Age'] = row[10]

			# Fighter 1 Statistics Information

			sql = """ SELECT sdbl_a, sdhl_a, sdll_a, tsl, tsa, ssl, ssa, sa, kd, 
							 scbl, scba, schl, scha, scll, scla, rv, sr, tdl, tda, tds, 
							 sgbl, sgba, sghl, sgha, sgll, sgla, ad, adtb, adhg, adtm, adts, sm 
								FROM StandingStatistics, ClinchStatistics, GroundStatistics 
								WHERE StandingStatistics.id=? AND ClinchStatistics.id=? AND GroundStatistics.id=? 
								AND StandingStatistics.match_date=? AND ClinchStatistics.match_date=? AND GroundStatistics.match_date=?
			"""

			val = (row[6], row[6], row[6], row[0], row[0], row[0])

			try:
				sql_result = cursor.execute(sql, val).fetchone()
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

					dictionary['F1SCBL'] = sql_result[9]
					dictionary['F1SCBA'] = sql_result[10]
					dictionary['F1SCHL'] = sql_result[11]
					dictionary['F1SCHA'] = sql_result[12]
					dictionary['F1SCLL'] = sql_result[13]
					dictionary['F1SCLA'] = sql_result[14]
					dictionary['F1RV'] = sql_result[15]
					dictionary['F1SR'] = sql_result[16]
					dictionary['F1TDL'] = sql_result[17]
					dictionary['F1TDA'] = sql_result[18]
					dictionary['F1TDS'] = sql_result[19]

					dictionary['F1SGBL'] = sql_result[20]
					dictionary['F1SGBA'] = sql_result[21]
					dictionary['F1SGHL'] = sql_result[22]
					dictionary['F1SGHA'] = sql_result[23]
					dictionary['F1SGLL'] = sql_result[24]
					dictionary['F1SGLA'] = sql_result[25]
					dictionary['F1AD'] = sql_result[26]
					dictionary['F1ADTB'] = sql_result[27]
					dictionary['F1ADHG'] = sql_result[28]
					dictionary['F1ADTM'] = sql_result[29]
					dictionary['F1ADTS'] = sql_result[30]
					dictionary['F1SM'] = sql_result[31]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get statistics information due to above error.")

			# fill up missing entries with empty string
			if 'F1SDBL' not in dictionary:
				dictionary['F1SDBL'] = ''
				dictionary['F1SDBA'] = ''
				dictionary['F1SDHL'] = ''
				dictionary['F1SDHA'] = ''
				dictionary['F1SDLL'] = ''
				dictionary['F1SDLA'] = ''
				dictionary['F1TSL'] = ''
				dictionary['F1TSA'] = ''
				dictionary['F1SSL'] = ''
				dictionary['F1SSA'] = ''
				dictionary['F1SA'] = ''
				dictionary['F1KD'] = ''

				dictionary['F1SCBL'] = ''
				dictionary['F1SCBA'] = ''
				dictionary['F1SCHL'] = ''
				dictionary['F1SCHA'] = ''
				dictionary['F1SCLL'] = ''
				dictionary['F1SCLA'] = ''
				dictionary['F1RV'] = ''
				dictionary['F1SR'] = ''
				dictionary['F1TDL'] = ''
				dictionary['F1TDA'] = ''
				dictionary['F1TDS'] = ''

				dictionary['F1SGBL'] = ''
				dictionary['F1SGBA'] = ''
				dictionary['F1SGHL'] = ''
				dictionary['F1SGHA'] = ''
				dictionary['F1SGLL'] = ''
				dictionary['F1SGLA'] = ''
				dictionary['F1AD'] = ''
				dictionary['F1ADTB'] = ''
				dictionary['F1ADHG'] = ''
				dictionary['F1ADTM'] = ''
				dictionary['F1ADTS'] = ''
				dictionary['F1SM'] = ''

			# Fighter 2 General Information
			
			sql = "SELECT id, name, height, reach, age FROM Fighters WHERE name=? and url=?"

			val = (str(row[12]).strip(), row[14] if row[14] is not None else str(''))

			try:
				sql_result = cursor.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F2Id'] = sql_result[0]
					dictionary['F2Name'] = sql_result[1]
					dictionary['F2Height'] = sql_result[2]
					dictionary['F2Reach'] = sql_result[3]
					dictionary['F2Age'] = sql_result[4]
				# else:
				# 	print(str(row[12]).strip(), sql, val)
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get fighter 2 information due to above error.")

			if 'F2Id' not in dictionary: # skip over if identifier of fighter 2 is not avaiable
				self.rows_for_schema.append({})
				self.get_rows_bar.update(len(self.rows_for_schema))
				continue

			# Fighter 2 Statistics Information

			sql = """ SELECT sdbl_a, sdhl_a, sdll_a, tsl, tsa, ssl, ssa, sa, kd, 
							 scbl, scba, schl, scha, scll, scla, rv, sr, tdl, tda, tds, 
							 sgbl, sgba, sghl, sgha, sgll, sgla, ad, adtb, adhg, adtm, adts, sm 
								FROM StandingStatistics, ClinchStatistics, GroundStatistics 
								WHERE StandingStatistics.id=? AND ClinchStatistics.id=? AND GroundStatistics.id=? 
								AND StandingStatistics.match_date=? AND ClinchStatistics.match_date=? AND GroundStatistics.match_date=?
			"""

			val = (dictionary['F2Id'], dictionary['F2Id'], dictionary['F2Id'], row[0], row[0], row[0])

			try:
				sql_result = cursor.execute(sql, val).fetchone()
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

					dictionary['F2SCBL'] = sql_result[9]
					dictionary['F2SCBA'] = sql_result[10]
					dictionary['F2SCHL'] = sql_result[11]
					dictionary['F2SCHA'] = sql_result[12]
					dictionary['F2SCLL'] = sql_result[13]
					dictionary['F2SCLA'] = sql_result[14]
					dictionary['F2RV'] = sql_result[15]
					dictionary['F2SR'] = sql_result[16]
					dictionary['F2TDL'] = sql_result[17]
					dictionary['F2TDA'] = sql_result[18]
					dictionary['F2TDS'] = sql_result[19]

					dictionary['F2SGBL'] = sql_result[20]
					dictionary['F2SGBA'] = sql_result[21]
					dictionary['F2SGHL'] = sql_result[22]
					dictionary['F2SGHA'] = sql_result[23]
					dictionary['F2SGLL'] = sql_result[24]
					dictionary['F2SGLA'] = sql_result[25]
					dictionary['F2AD'] = sql_result[26]
					dictionary['F2ADTB'] = sql_result[27]
					dictionary['F2ADHG'] = sql_result[28]
					dictionary['F2ADTM'] = sql_result[29]
					dictionary['F2ADTS'] = sql_result[30]
					dictionary['F2SM'] = sql_result[31]
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get Statistics information due to above error(F2).")

			# fill up missing entries with empty string
			if 'F2SDBL' not in dictionary:
				dictionary['F2SDBL'] = ''
				dictionary['F2SDBA'] = ''
				dictionary['F2SDHL'] = ''
				dictionary['F2SDHA'] = ''
				dictionary['F2SDLL'] = ''
				dictionary['F2SDLA'] = ''
				dictionary['F2TSL'] = ''
				dictionary['F2TSA'] = ''
				dictionary['F2SSL'] = ''
				dictionary['F2SSA'] = ''
				dictionary['F2SA'] = ''
				dictionary['F2KD'] = ''

				dictionary['F2SCBL'] = ''
				dictionary['F2SCBA'] = ''
				dictionary['F2SCHL'] = ''
				dictionary['F2SCHA'] = ''
				dictionary['F2SCLL'] = ''
				dictionary['F2SCLA'] = ''
				dictionary['F2RV'] = ''
				dictionary['F2SR'] = ''
				dictionary['F2TDL'] = ''
				dictionary['F2TDA'] = ''
				dictionary['F2TDS'] = ''

				dictionary['F2SGBL'] = ''
				dictionary['F2SGBA'] = ''
				dictionary['F2SGHL'] = ''
				dictionary['F2SGHA'] = ''
				dictionary['F2SGLL'] = ''
				dictionary['F2SGLA'] = ''
				dictionary['F2AD'] = ''
				dictionary['F2ADTB'] = ''
				dictionary['F2ADHG'] = ''
				dictionary['F2ADTM'] = ''
				dictionary['F2ADTS'] = ''
				dictionary['F2SM'] = ''

			# check if duplicates exist
			duplicate_list = [r for r in self.rows_for_schema if 'Date' in r and r['Date'] == dictionary['Date'] and r['Winner'] == dictionary['Winner'] 
									and r['Time'] == dictionary['Time'] and r['IsTitle?'] == dictionary['IsTitle?'] and r['DecisionType'] == dictionary['DecisionType']]

			# the dictionary is filled up, let's add it into the list
			# if duplicated, append empty dict
			if len(duplicate_list) == 0:
				self.rows_for_schema.append(dictionary)
			else:
				self.rows_for_schema.append({})
			
			self.get_rows_bar.update(len(self.rows_for_schema))

		cursor.close()

		conn_.close()

		# check whether all threads are finished and then write to excel
		# remove temp files and folder
		if len(self.rows_for_schema) >= self.total_row_count:
			self.write_to_excel(self.rows_for_schema)
			rmtree(self.tmp_dir)


	def get_rows_for_schema(self):
		""" get all rows to be written onto the excel file
		param:
		return:
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

		print('Getting rows for excel output from database...')

		# fetch queried result
		rows = self.c.execute(sql).fetchall()

		# close current connection to database since we no more need it
		self.close_connection()

		# check if there's result to be considered
		if len(rows) == 0:
			print('Cannot get information for excel output from database')
			return

		# this is used to check if all threads are finished
		self.total_row_count = len(rows)

		# shows the progress of total processing
		self.get_rows_bar = progressbar.ProgressBar(maxval=len(rows), \
									widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(rows))])

		# self.total_row_count = 1000
		# self.get_rows_bar = progressbar.ProgressBar(maxval=1000, \
		# 							widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', '1000'])

		self.get_rows_bar.start()

		# devide the rows into smaller pieces to load them on threads
		tmp_list = [rows[x:x + int(len(rows) / self.thread_count)] for x in range(0, len(rows) - 1, int(len(rows) / self.thread_count))]
		# tmp_list = [rows[x:x + int(1000 / self.thread_count)] for x in range(0, 999, int(1000 / self.thread_count))]

		# ensure the temp directory does exist
		if not os.path.exists(self.tmp_dir):
			os.mkdir(self.tmp_dir)

		# now the threads
		# copy current database to temp directory and rename it with indices
		# each thread works on the thread which is named with thread's index
		for index, list_ in enumerate(tmp_list):

			# path for temp db file
			db_path = os.path.join(self.tmp_dir, f'tmp_{index + 1}.db')

			# make a copy of current database
			copyfile(self.db_file_, db_path)

			thread_ = threading.Thread(target=self.get_rows, args=(index, list_, db_path))

			thread_.start()


if __name__ == "__main__":

	db = UFCHistoryDB('ufc_history.db')
	db.get_rows_for_schema()

	# print("Please run the main script!")