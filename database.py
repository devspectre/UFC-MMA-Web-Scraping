
import sqlite3
import os
import sys
import threading
import progressbar
import pickle
from shutil import copyfile
from shutil import rmtree
from excel import ExcelWriter
from datetime import datetime as DT
from collections import Counter

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

		# thread_counter, it is increased by 1 when a 
		self.thread_counter = 0

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

	def write_to_excel(self, rows, file_name = 'ufc_history'):
		""" writes rows to excel
		param rows: a list of dictionaries
		return: number of rows written successfully
		"""

		# create an excel writer instance
		xw = ExcelWriter(file_name)
		
		# create horizontal header
		xw.set_header_list(xw.header_list)

		# initialize variables row_index, our default template starts from row 4
		row_index = 4

		xw_bar = progressbar.ProgressBar(maxval=len(rows), \
										widgets=['EXCEL:', progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(rows))])
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

		return row_index

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
				self.rows_for_schema.append({})
				self.get_rows_bar.update(len(self.rows_for_schema))
				continue

			# dictionary to contain match information
			dictionary = {}

			# General Info (From Fight History Page)
			dictionary['Date'] = row[0].strip()
			dictionary['WeightClass'] = row[1].strip() if row[1] is not None else None

			if row[13] == 'Win':
				dictionary['Winner'] = row[7].strip()
			elif row[13] == 'Loss':
				dictionary['Winner'] = row[12].strip()
			else:
				dictionary['Winner'] = ''

			dictionary['DecisionType'] = row[2].strip()
			dictionary['Rounds'] = row[3]
			dictionary['Time'] = row[4].strip()
			dictionary['IsTitle?'] = row[5].strip()

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
								AND StandingStatistics.opp_url=? AND ClinchStatistics.opp_url=? AND GroundStatistics.opp_url=?
			"""

			val = (row[6], row[6], row[6], row[0], row[0], row[0], row[14], row[14], row[14])

			try:
				sql_result = cursor.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:

					dictionary['F1SDBL'] = int(sql_result[0].split('/')[0]) if '/' in sql_result[0] else 0
					dictionary['F1SDBA'] = int(sql_result[0].split('/')[1]) if '/' in sql_result[0] else 0
					dictionary['F1SDHL'] = int(sql_result[1].split('/')[0]) if '/' in sql_result[1] else 0
					dictionary['F1SDHA'] = int(sql_result[1].split('/')[1]) if '/' in sql_result[1] else 0
					dictionary['F1SDLL'] = int(sql_result[2].split('/')[0]) if '/' in sql_result[2] else 0
					dictionary['F1SDLA'] = int(sql_result[2].split('/')[1]) if '/' in sql_result[2] else 0
					dictionary['F1TSL'] = UFCHistoryDB.atoi(sql_result[3])
					dictionary['F1TSA'] = UFCHistoryDB.atoi(sql_result[4])
					dictionary['F1SSL'] = UFCHistoryDB.atoi(sql_result[5])
					dictionary['F1SSA'] = UFCHistoryDB.atoi(sql_result[6])
					dictionary['F1SA'] = UFCHistoryDB.atoi(sql_result[7])
					dictionary['F1KD'] = UFCHistoryDB.atoi(sql_result[8])

					dictionary['F1SCBL'] = UFCHistoryDB.atoi(sql_result[9])
					dictionary['F1SCBA'] = UFCHistoryDB.atoi(sql_result[10])
					dictionary['F1SCHL'] = UFCHistoryDB.atoi(sql_result[11])
					dictionary['F1SCHA'] = UFCHistoryDB.atoi(sql_result[12])
					dictionary['F1SCLL'] = UFCHistoryDB.atoi(sql_result[13])
					dictionary['F1SCLA'] = UFCHistoryDB.atoi(sql_result[14])
					dictionary['F1RV'] = UFCHistoryDB.atoi(sql_result[15])
					dictionary['F1SR'] = UFCHistoryDB.atoi(sql_result[16])
					dictionary['F1TDL'] = UFCHistoryDB.atoi(sql_result[17])
					dictionary['F1TDA'] = UFCHistoryDB.atoi(sql_result[18])
					dictionary['F1TDS'] = UFCHistoryDB.atoi(sql_result[19])

					dictionary['F1SGBL'] = UFCHistoryDB.atoi(sql_result[20])
					dictionary['F1SGBA'] = UFCHistoryDB.atoi(sql_result[21])
					dictionary['F1SGHL'] = UFCHistoryDB.atoi(sql_result[22])
					dictionary['F1SGHA'] = UFCHistoryDB.atoi(sql_result[23])
					dictionary['F1SGLL'] = UFCHistoryDB.atoi(sql_result[24])
					dictionary['F1SGLA'] = UFCHistoryDB.atoi(sql_result[25])
					dictionary['F1AD'] = UFCHistoryDB.atoi(sql_result[26])
					dictionary['F1ADTB'] = UFCHistoryDB.atoi(sql_result[27])
					dictionary['F1ADHG'] = UFCHistoryDB.atoi(sql_result[28])
					dictionary['F1ADTM'] = UFCHistoryDB.atoi(sql_result[29])
					dictionary['F1ADTS'] = UFCHistoryDB.atoi(sql_result[30])
					dictionary['F1SM'] = UFCHistoryDB.atoi(sql_result[31])

					# if int(dictionary['F1Id']) == 12118:
					# 	print(dictionary['Date'], dictionary['F1Id'], dictionary['F1Name'], dictionary['F1SDBL'], dictionary['F1SDBA'])
					# 	print()
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get statistics information due to above error.")

			# fill up missing entries with empty string
			if 'F1SDBL' not in dictionary:
				dictionary['F1SDBL'] = 0
				dictionary['F1SDBA'] = 0
				dictionary['F1SDHL'] = 0
				dictionary['F1SDHA'] = 0
				dictionary['F1SDLL'] = 0
				dictionary['F1SDLA'] = 0
				dictionary['F1TSL'] = 0
				dictionary['F1TSA'] = 0
				dictionary['F1SSL'] = 0
				dictionary['F1SSA'] = 0
				dictionary['F1SA'] = 0
				dictionary['F1KD'] = 0

				dictionary['F1SCBL'] = 0
				dictionary['F1SCBA'] = 0
				dictionary['F1SCHL'] = 0
				dictionary['F1SCHA'] = 0
				dictionary['F1SCLL'] = 0
				dictionary['F1SCLA'] = 0
				dictionary['F1RV'] = 0
				dictionary['F1SR'] = 0
				dictionary['F1TDL'] = 0
				dictionary['F1TDA'] = 0
				dictionary['F1TDS'] = 0

				dictionary['F1SGBL'] = 0
				dictionary['F1SGBA'] = 0
				dictionary['F1SGHL'] = 0
				dictionary['F1SGHA'] = 0
				dictionary['F1SGLL'] = 0
				dictionary['F1SGLA'] = 0
				dictionary['F1AD'] = 0
				dictionary['F1ADTB'] = 0
				dictionary['F1ADHG'] = 0
				dictionary['F1ADTM'] = 0
				dictionary['F1ADTS'] = 0
				dictionary['F1SM'] = 0

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
								AND StandingStatistics.opp_url=? AND ClinchStatistics.opp_url=? AND GroundStatistics.opp_url=?
			"""

			val = (dictionary['F2Id'], dictionary['F2Id'], dictionary['F2Id'], row[0], row[0], row[0], row[11], row[11], row[11])

			try:
				sql_result = cursor.execute(sql, val).fetchone()
				if sql_result is not None and len(sql_result) > 0:
					dictionary['F2SDBL'] = int(sql_result[0].split('/')[0]) if '/' in sql_result[0] else 0
					dictionary['F2SDBA'] = int(sql_result[0].split('/')[1]) if '/' in sql_result[0] else 0
					dictionary['F2SDHL'] = int(sql_result[1].split('/')[0]) if '/' in sql_result[1] else 0
					dictionary['F2SDHA'] = int(sql_result[1].split('/')[1]) if '/' in sql_result[1] else 0
					dictionary['F2SDLL'] = int(sql_result[2].split('/')[0]) if '/' in sql_result[2] else 0
					dictionary['F2SDLA'] = int(sql_result[2].split('/')[1]) if '/' in sql_result[2] else 0
					dictionary['F2TSL'] = UFCHistoryDB.atoi(sql_result[3])
					dictionary['F2TSA'] = UFCHistoryDB.atoi(sql_result[4])
					dictionary['F2SSL'] = UFCHistoryDB.atoi(sql_result[5])
					dictionary['F2SSA'] = UFCHistoryDB.atoi(sql_result[6])
					dictionary['F2SA'] = UFCHistoryDB.atoi(sql_result[7])
					dictionary['F2KD'] = UFCHistoryDB.atoi(sql_result[8])

					dictionary['F2SCBL'] = UFCHistoryDB.atoi(sql_result[9])
					dictionary['F2SCBA'] = UFCHistoryDB.atoi(sql_result[10])
					dictionary['F2SCHL'] = UFCHistoryDB.atoi(sql_result[11])
					dictionary['F2SCHA'] = UFCHistoryDB.atoi(sql_result[12])
					dictionary['F2SCLL'] = UFCHistoryDB.atoi(sql_result[13])
					dictionary['F2SCLA'] = UFCHistoryDB.atoi(sql_result[14])
					dictionary['F2RV'] = UFCHistoryDB.atoi(sql_result[15])
					dictionary['F2SR'] = UFCHistoryDB.atoi(sql_result[16])
					dictionary['F2TDL'] = UFCHistoryDB.atoi(sql_result[17])
					dictionary['F2TDA'] = UFCHistoryDB.atoi(sql_result[18])
					dictionary['F2TDS'] = UFCHistoryDB.atoi(sql_result[19])

					dictionary['F2SGBL'] = UFCHistoryDB.atoi(sql_result[20])
					dictionary['F2SGBA'] = UFCHistoryDB.atoi(sql_result[21])
					dictionary['F2SGHL'] = UFCHistoryDB.atoi(sql_result[22])
					dictionary['F2SGHA'] = UFCHistoryDB.atoi(sql_result[23])
					dictionary['F2SGLL'] = UFCHistoryDB.atoi(sql_result[24])
					dictionary['F2SGLA'] = UFCHistoryDB.atoi(sql_result[25])
					dictionary['F2AD'] = UFCHistoryDB.atoi(sql_result[26])
					dictionary['F2ADTB'] = UFCHistoryDB.atoi(sql_result[27])
					dictionary['F2ADHG'] = UFCHistoryDB.atoi(sql_result[28])
					dictionary['F2ADTM'] = UFCHistoryDB.atoi(sql_result[29])
					dictionary['F2ADTS'] = UFCHistoryDB.atoi(sql_result[30])
					dictionary['F2SM'] = UFCHistoryDB.atoi(sql_result[31])
			except Exception as e:
				print(f'Error(DB.get_rows_for_schema): {str(e)}')
				print("Cannot get Statistics information due to above error(F2).")

			# fill up missing entries with empty string
			if 'F2SDBL' not in dictionary:
				dictionary['F2SDBL'] = 0
				dictionary['F2SDBA'] = 0
				dictionary['F2SDHL'] = 0
				dictionary['F2SDHA'] = 0
				dictionary['F2SDLL'] = 0
				dictionary['F2SDLA'] = 0
				dictionary['F2TSL'] = 0
				dictionary['F2TSA'] = 0
				dictionary['F2SSL'] = 0
				dictionary['F2SSA'] = 0
				dictionary['F2SA'] = 0
				dictionary['F2KD'] = 0

				dictionary['F2SCBL'] = 0
				dictionary['F2SCBA'] = 0
				dictionary['F2SCHL'] = 0
				dictionary['F2SCHA'] = 0
				dictionary['F2SCLL'] = 0
				dictionary['F2SCLA'] = 0
				dictionary['F2RV'] = 0
				dictionary['F2SR'] = 0
				dictionary['F2TDL'] = 0
				dictionary['F2TDA'] = 0
				dictionary['F2TDS'] = 0

				dictionary['F2SGBL'] = 0
				dictionary['F2SGBA'] = 0
				dictionary['F2SGHL'] = 0
				dictionary['F2SGHA'] = 0
				dictionary['F2SGLL'] = 0
				dictionary['F2SGLA'] = 0
				dictionary['F2AD'] = 0
				dictionary['F2ADTB'] = 0
				dictionary['F2ADHG'] = 0
				dictionary['F2ADTM'] = 0
				dictionary['F2ADTS'] = 0
				dictionary['F2SM'] = 0

			self.rows_for_schema.append(dictionary)
			
			self.get_rows_bar.update(len(self.rows_for_schema))

		cursor.close()

		conn_.close()

		self.thread_counter += 1

		# check whether all threads are finished and then write to excel
		# remove temp files and folder

		if self.thread_counter >= self.thread_count:
			self.get_rows_bar.update(self.total_row_count)
			self.get_rows_bar.finish()
			print('All threads are finished!')

			# get rid of duplicates
			done = []
			result = []
			for row in self.rows_for_schema:
				if 'Date' not in row: # skip over empty row
					continue
				# NOTE: need to pay attention to picking those keys to remove duplicates 
				# 		and not to remove different matches on the same date
				d = (row['Date'], row['Winner'], row['Time'])
				if d not in done:
					done.append(d)
					result.append(row)

			# sort list by date
			result = sorted(result, key = lambda x : (x['Date'], x['Winner'], x['IsTitle?']))

			try:
				self.write_to_excel(result)
			except Exception as e:
				print(f'Failed to write to excel due to error: {str(e)}')
			
			self.write_match_history(result, is_sum = True, write_to_db = True)

			try:
				UFCHistoryDB.write_pickle_file(result)
			except Exception as e:
				print(f'Failed to write pickle file due to error: {str(e)}')

			try:
				rmtree(self.tmp_dir)
			except Exception as e:
				pass

			print(f'{len(result)} matches are registered!')

			# code for test unpickling
			# unpickled_data = UFCHistoryDB.read_pickle_file()
			# for row in unpickled_data:
			# 	print(row)
			# 	print()

	def write_match_history(self, rows, is_sum = False, write_to_db = False, db_name = 'match_history.db'):
		""" write match history to a database
		param rows: actual data list
		param db_name: match history database name
		param is_sum: True: get sum of each statistics value up to the match point, False: just write the statistic value of that match
		return:
		"""

		rows_ = []

		if is_sum:

			print('Doing the sum on statistics...')

			sum_bar = progressbar.ProgressBar(maxval=len(rows), \
									widgets=['SUM:', progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(rows))])

			sum_bar.start()

			royce_list = []
			royce_sum = []

			index = 0

			for index, row in enumerate(rows): # iterate over the list
				if row is None or len(row) == 0:
					# update progress bar and skip over
					index += 1
					sum_bar.update(index)
					continue

				try:

					result = {}
					
					is_fighter1_done = False
					is_fighter2_done = False

					# seek for the last matching match history of both fighters(fighter1 and fighter2) in reversed order
					# make sure that the source list is already sorted by date
					for r in reversed(rows_):
						if not is_fighter1_done: # need to seek for fighter 1's last history
							if r['F1Id'] == row['F1Id'] and not (r['Date'] == row['Date'] and r['Winner'] == row['Winner'] and r['Time'] == row['Time']):
								for key, value in r.items():
									if key.startswith('F1') and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
										result[key] = value

								is_fighter1_done = True

							if r['F2Id'] == row['F1Id'] and not (r['Date'] == row['Date'] and r['Winner'] == row['Winner'] and r['Time'] == row['Time']):
								for key, value in r.items():
									if key.startswith('F2') and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
										result[key.replace('F2', 'F1')] = value	

								is_fighter1_done = True

						if not is_fighter2_done: # need to seek for fighter 2's last history
							if r['F1Id'] == row['F2Id'] and not (r['Date'] == row['Date'] and r['Winner'] == row['Winner'] and r['Time'] == row['Time']):
								for key, value in r.items():
									if key.startswith('F1') and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
										result[key.replace('F1', 'F2')] = value

								is_fighter2_done = True

							if r['F2Id'] == row['F2Id'] and not (r['Date'] == row['Date'] and r['Winner'] == row['Winner'] and r['Time'] == row['Time']):
								for key, value in r.items():
									if key.startswith('F2') and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
										result[key] = value

								is_fighter2_done = True

						if is_fighter1_done and is_fighter2_done:
							break

					# no prior match history, only need to consider current statistics
					if result is None or len(result) == 0:
						result = row
					else:

						if 'F1SDBL' not in result: # fighter 1's history was not found
							for key, value in row.items():
								if key.startswith('F2') and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
									result[key] += value
								else:
									result[key] = value

						elif 'F2SDBL' not in result: # fighter 2's history was not found
							for key, value in row.items():
								if key.startswith('F1') and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
									result[key] += value
								else:
									result[key] = value

						else:
							# sum up prior statistics with current one
							for key, value in row.items():
								if (key.startswith('F1') or key.startswith('F2')) and not (key.endswith('Name') or key.endswith('Height') or key.endswith('Reach') or key.endswith('Reach') or key.endswith('Age') or key.endswith('Id')):
									result[key] += value
								else:
									result[key] = value

				except Exception as e:
					print(f'Exception while getting sums(DB.write_match_history_to_db): {str(e)}')
					continue

				# royce gracie for test
				# if row['F1Id'] == 8011 or row['F2Id'] == 8011:
				# 	royce_list.append(row)
				# 	royce_sum.append(result)

				rows_.append(result)
				# update progress bar
				index += 1
				sum_bar.update(index)

			sum_bar.finish()

			print('Doing the sum of statistics is done!')
		else:
			rows_ = rows

		# write sumed rows to excel
		try:
			self.write_to_excel(rows_, 'ufc_history_sum')
			self.write_to_excel(royce_list, 'royce_history')
			self.write_to_excel(royce_sum, 'royce_sum')
		except Exception as e:
			print(f'Failed to write excel file: {str(e)}')
		

		# write match history into database
		if write_to_db:
			try:
				os.remove(db_name)
			except Exception as e:
				# print(f'Failed to remove old db file: {str(e)}')
				pass

			conn_ = None
			cursor = None

			try:
				conn_ = sqlite3.connect(db_name)
				cursor = conn_.cursor()
			except Exception as e:
				print(f'Exception(DB.write_match_history_to_db): Cannot connect to database {db_name} : {str(e)}')
				return

			if conn_ is None or cursor is None:
				print('Failed to connect database(DB.write_match_history_to_db)')
				return

			cursor.execute("""CREATE TABLE IF NOT EXISTS MatchHistory (
						match_id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
						match_date text NOT NULL,
						weight_class text,
						winner text,
						decision_type text,
						rounds integer,
						match_time text,
						is_title text,

						f1id integer,
						f1name text,
						f1height text,
						f1reach text,
						f1age integer,
						f1sdbl integer,
						f1sdba integer,
						f1sdhl integer,
						f1sdha integer,
						f1sdll integer,
						f1sdla integer,
						f1tsl integer,
						f1tsa integer,
						f1ssl integer,
						f1ssa integer,
						f1sa integer,
						f1kd integer,

						f1scbl integer,
						f1scba integer,
						f1schl integer,
						f1scha integer,
						f1scll integer,
						f1scla integer,
						f1rv integer,
						f1sr integer,
						f1tdl integer,
						f1tda integer,
						f1tds integer,

						f1sgbl integer,
						f1sgba integer,
						f1sghl integer,
						f1sgha integer,
						f1sgll integer,
						f1sgla integer,
						f1ad integer,
						f1adtb integer,
						f1adhg integer,
						f1adtm integer,
						f1adts integer,
						f1sm integer,

						f2id integer,
						f2name text,
						f2height text,
						f2reach text,
						f2age integer,

						f2sdbl integer,
						f2sdba integer,
						f2sdhl integer,
						f2sdha integer,
						f2sdll integer,
						f2sdla integer,
						f2tsl integer,
						f2tsa integer,
						f2ssl integer,
						f2ssa integer,
						f2sa integer,
						f2kd integer,

						f2scbl integer,
						f2scba integer,
						f2schl integer,
						f2scha integer,
						f2scll integer,
						f2scla integer,
						f2rv integer,
						f2sr integer,
						f2tdl integer,
						f2tda integer,
						f2tds integer,

						f2sgbl integer,
						f2sgba integer,
						f2sghl integer,
						f2sgha integer,
						f2sgll integer,
						f2sgla integer,
						f2ad integer,
						f2adtb integer,
						f2adhg integer,
						f2adtm integer,
						f2adts integer,
						f2sm integer
						)""")

			cursor.close()
			conn_.close()

			try:
				conn_ = sqlite3.connect(db_name)
				cursor = conn_.cursor()
			except Exception as e:
				print(f'Exception while reconnecting to database(DB.write_match_history_to_db): {str(e)}')
				return

			# bulk insert to database

			sql = """ INSERT INTO MatchHistory (match_date, weight_class, winner, decision_type, rounds, match_time, is_title,
									f1id, f1name, f1height,	f1reach, f1age,
									f1sdbl, f1sdba, f1sdhl, f1sdha, f1sdll, f1sdla, f1tsl, f1tsa, f1ssl, f1ssa, f1sa, f1kd,
									f1scbl, f1scba, f1schl, f1scha, f1scll,	f1scla, f1rv, f1sr, f1tdl, f1tda, f1tds,
									f1sgbl, f1sgba, f1sghl, f1sgha, f1sgll, f1sgla, f1ad, f1adtb, f1adhg, f1adtm, f1adts, f1sm,
									f2id, f2name, f2height, f2reach, f2age,
									f2sdbl, f2sdba, f2sdhl, f2sdha, f2sdll, f2sdla, f2tsl, f2tsa, f2ssl, f2ssa, f2sa, f2kd,
									f2scbl, f2scba, f2schl, f2scha, f2scll,	f2scla, f2rv, f2sr, f2tdl, f2tda, f2tds,
									f2sgbl, f2sgba, f2sghl, f2sgha, f2sgll, f2sgla, f2ad, f2adtb, f2adhg, f2adtm, f2adts, f2sm)
							 VALUES (?, ?, ?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
							 		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			"""

			cursor.execute('BEGIN TRANSACTION')
			for row in rows_:
				if row is None or len(row) == 0:
					continue
				try:
					val = (row['Date'], row['WeightClass'], row['Winner'],	row['DecisionType'], row['Rounds'], row['Time'], row['IsTitle?'],
						row['F1Id'], row['F1Name'], row['F1Height'], row['F1Reach'], row['F1Age'],
						row['F1SDBL'], row['F1SDBA'], row['F1SDHL'], row['F1SDHA'], row['F1SDLL'], row['F1SDLA'], row['F1TSL'], row['F1TSA'], row['F1SSL'], row['F1SSA'], row['F1SA'], row['F1KD'],
						row['F1SCBL'], row['F1SCBA'], row['F1SCHL'], row['F1SCHA'], row['F1SCLL'], row['F1SCLA'], row['F1RV'], row['F1SR'], row['F1TDL'], row['F1TDA'], row['F1TDS'], 
						row['F1SGBL'], row['F1SGBA'], row['F1SGHL'], row['F1SGHA'], row['F1SGLL'], row['F1SGLA'], row['F1AD'], row['F1ADTB'], row['F1ADHG'], row['F1ADTM'], row['F1ADTS'], row['F1SM'],
						row['F2Id'], row['F2Name'], row['F2Height'], row['F2Reach'], row['F2Age'],
						row['F2SDBL'], row['F2SDBA'], row['F2SDHL'], row['F2SDHA'], row['F2SDLL'], row['F2SDLA'], row['F2TSL'], row['F2TSA'], row['F2SSL'], row['F2SSA'], row['F2SA'], row['F2KD'],
						row['F2SCBL'], row['F2SCBA'], row['F2SCHL'], row['F2SCHA'], row['F2SCLL'], row['F2SCLA'], row['F2RV'], row['F2SR'], row['F2TDL'], row['F2TDA'], row['F2TDS'], 
						row['F2SGBL'], row['F2SGBA'], row['F2SGHL'], row['F2SGHA'], row['F2SGLL'], row['F2SGLA'], row['F2AD'], row['F2ADTB'], row['F2ADHG'], row['F2ADTM'], row['F2ADTS'], row['F2SM'],
						)
				except Exception as e:
					print(f'Exception while making query(DB.write_match_history_to_db): {str(e)}')
					print(row)
					continue
				
				cursor.execute(sql, val)

			cursor.execute('COMMIT')
		print('Writing match history done!')

	@staticmethod
	def write_pickle_file(rows, file_name = 'match_history_sum'):
		""" write given data(rows) to the file(file_name)
		param rows: source data
		param file_name: name of pickle file
		return: True if successful, otherwise False
		"""

		outfile = None

		try:
			outfile= open(file_name, 'wb') # open the file in write and binary mode to write in form of byte objects
		except Exception as e:
			print(f'Failed to write pickle file. Cannot open the file {file_name}. {str(e)}')
			return False
		
		try:
			print('Writing data to pickle file...')
			pickle.dump(rows, outfile) # dump data to the pickle file
		except Exception as e:
			print(f'Failed to dump data to pickle file. {str(e)}')
			outfile.close() # do not forget to close opened file
			return False

		outfile.close() # close the file to complete writing

		print(f'Writing data to pickle file is done. File: {file_name}')
		return True

	@staticmethod
	def read_pickle_file(file_name = 'match_history_sum'):
		""" read pickle file and retrieve data
		param file_name: name of pickle file
		return: list of data
		"""

		infile = None # file handle

		try:
			infile = open(file_name, 'rb') # read file in read and binary mode
		except Exception as e: # NOTE: Check whether the retrieved data is empty or not for this exception in caller of this method
			print(f'Failed to read pickle file. Cannot open the file {file_name}. {str(e)}')
			return None

		data_list = [] # list of data to be returned

		try:
			print('Reading data from pickled file...')
			data_list = pickle.load(infile) # retrieve data from pickled file
		except Exception as e:
			print(f'Failed to retrieve data from pickled file. {str(e)}')
			return None
		
		infile.close() # close the file

		print(f'Retrieved {len(data_list)} rows from pickled file.') # log

		return data_list

	def get_rows_for_schema(self):
		""" get all rows to be written to the excel file
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
									widgets=['QUERYING DB:', progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(rows))])

		# self.total_row_count = 1000
		# self.get_rows_bar = progressbar.ProgressBar(maxval=1000, \
									# widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', '1000'])

		self.get_rows_bar.start()

		# devide the rows into smaller pieces to load them on threads
		tmp_list = [rows[x:x + int(len(rows) / (self.thread_count - 1))] for x in range(0, len(rows) - 1, int(len(rows) / (self.thread_count - 1)))]
		# tmp_list = [rows[x:x + int(1000 / self.thread_count)] for x in range(0, 999, int(1000 / self.thread_count))]

		# ensure thread_count equals to the number of sub lists in tmp_list
		self.thread_count = len(tmp_list)

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