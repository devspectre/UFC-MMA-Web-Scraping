import sys, getopt
import requests
import string
import threading
import signal
import progressbar
from bs4 import BeautifulSoup
from datetime import datetime as DT

import database
from excel import ExcelWriter

# # global variable for progressbar
# bar = None

def list_to_string(list_, delimiter):
	""" returns a string from given list joined with given delimiter
	param list_: source list
	param delimiter: delimiter which goes between strings to combine them into a string
	return: a string which is combined with all strings in the list by delimiter
	"""

	return f'{delimiter}'.join(str(element) for element in list_)

def get_page_url(url, page_name):
	""" this function retrieve url of fighter's stat page from profile url.
	param url: profile url
	param page_name: string to represent page name
	return: url of desired page
	"""
	
	prefix = url.split('/')[:5]

	suffix = url.split('/')[5:]

	# no more work if prefix or suffix is empty
	if prefix is None or suffix is None:
		return None
	# get prefix string from prefix list joined with delimiter
	prefix_str = list_to_string(prefix, '/')

	# get suffix string from suffix list joined with delimiter
	suffix_str = list_to_string(suffix, '/')

	return  prefix_str + '/' + page_name + '/' + suffix_str

def get_fighter_url_list_startwith(start_ch):
	""" returns a list of urls
		urls of all fighters whose name start with start_ch
	param start_ch: a character which is at the very first of names
	return: list of fighters whose names starts with 'start_ch'
	"""

	source = requests.get(f'http://www.espn.com/mma/fighters?search={start_ch}').text

	soup = BeautifulSoup(source, 'lxml')

	# get table content from 'table' tag
	tbl_content = soup.find('table')

	# create empty list
	fighter_list = []

	# find all trs from table content, filtered by classnames 'oddrow' and 'evenrow'
	for tr in tbl_content.find_all('tr', class_=['oddrow', 'evenrow']):
		# get url from anchor tag with property 'href'
		sub_link = tr.a['href']

		# attach main site url
		link = f'http://www.espn.com{sub_link}'

		# append the link to the list
		fighter_list.append(link)

	return fighter_list

def get_general_info(soup):
	""" returns a dictionary of fighter's general info
	param soup: soup object
	return: dictionary of general info
	"""
	
	# initialize info dictionary
	info_list = {}

	# get general info ul
	general_info = soup.find('ul', class_='general-info')

	if general_info is None:
		return info_list

	# print(general_info)

	# initialize variables
	name = None
	age = None
	weight_class = None
	height = None
	weight = None
	reach = None
	group_name = None

	try:
		name = soup.find('div', class_='mod-content').find('h1').text
	except Exception as e:
		pass

	if name is None:
		try:
			name = soup.find('div', class_='player-bio').find('h1').text
		except Exception as e:
			pass 

	try:
		tmp = general_info.find('li', class_="first last").text
		if tmp.find('\"') != -1 or tmp.find("lbs") != -1:
			if tmp.find(",") != -1:
				height = tmp.split(",")[0]
				weight = tmp.split(",")[1].strip()
			else:
				if tmp.find("lbs") != -1:
					weight = tmp
				else:
					height = tmp
		else:
			weight_class = tmp
	except Exception as e:
		pass

	try:
		item = general_info.find('li', class_='first')
		if len(item['class']) == 1:
			weight_class = item.text
	except Exception as e:
		pass

	try:
		tmp = general_info.find(class_=None).text
		if tmp.find('\"') != -1:
			if tmp.find("lbs") != -1 and tmp.find(",") != -1:
				height = tmp.split(',')[0]
				weight = tmp.split(',')[1].strip()
			else:
				height = tmp
		elif tmp.find("lbs") != -1:
			weight = tmp
	except Exception as e:
		pass

	try:
		item = general_info.find('li', class_='last')
		if len(item['class']) == 1:
			group_name = item.text
	except Exception as e:
		pass

	try:
		meta_data = soup.find('ul', class_='player-metadata')
		# print(meta_data)
		for li in meta_data.find_all('li'):
			try:
				span = li.find('span', text='Birth Date')
				try:
					age = (int)(li.text.split(":")[1].split(")")[0].strip())
				except Exception as e:
					pass
			except Exception as e:
				pass

			try:
				span = li.find('span', text='Reach')
				reach = li.text.split('Reach')[1].strip()
			except Exception as e:
				pass

	except Exception as e:
		pass

	info_list['name'] = name.strip()
	info_list['age'] = age
	info_list['reach'] = reach
	info_list['weight_class'] = weight_class
	info_list['height'] = height
	info_list['weight'] = weight
	info_list['group_name'] = group_name

	return info_list

def get_history_info(soup):
	""" returns a list of sub lists
		each sub list contains match _date, event, opponent, result, decision, rounds and time
	param soup: soup object
	return: list of dictionaries, each dictionary contains a single match info
	"""

	# get fight history information from the table
	#
	# !NOTE: ensure there's only one table body on the history page

	tbody = soup.find('table', class_='tablehead mod-player-stats')

	if tbody is None:
		# print("Cannot find table on the page")
		return []

	header_list = []
	header_columns = tbody.find('tr', class_='colhead').find_all('td')

	history_list = []

	for row in tbody.find_all('tr', class_=['oddrow', 'evenrow']):
		cells = row.find_all('td')

		if len(cells) != len(header_columns):
			# print("Warning(History): Column counts mismatch between header and rows!")
			continue

		if len(cells) == 0 or len(cells) == 1:
			# print("No item in the row!")
			continue
		else:
			index = 0
			history = {}
			for cell in cells:
				if header_columns[index].text == 'DATE':
					history[header_columns[index].text] = DT.strptime(cell.text, '%b %d, %Y').strftime('%Y-%m-%d')
				else:
					history[header_columns[index].text] = cell.text
				if cell.find('a') != None:
					history['opp_url'] = cell.find('a')['href']
				index += 1

			if len(history) > 0:
				history_list.append(history)

	return history_list

def get_statistics(soup):
	""" get standing statistics on stats page and returns a list of records
	param soup: soup object
	return: three lists - standing statistics, clinch statistics and ground statistics
	"""
	standing_list = []
	clinch_list = []
	ground_list = []

	for table in soup.find_all('table', class_='tablehead'):
		title = table.find('tr', class_='stathead').find('td').text

		if title == "STANDING STATISTICS":
			# get header labels to determine column counts and labels
			# this will allow you to scrap data without revising code 
			# even if the columns are changed in the future
			header_columns = []
			header = table.find('tr', class_='colhead').find_all('td')

			for column in header:
				header_columns.append(column.text.replace("%", "PERCENT"))
			
			# get statistics
			for row in table.find_all('tr', class_=['oddrow', 'evenrow']):# get rows of the table
				if len(row) == 0 or len(row) == 1:
					# print("No results for this statistics!")
					continue

				if len(row) != len(header_columns):
					# print("Warning: Columns mismatch!")
					continue

				# initialize variables repeatedly used
				index = 0
				drow = {}

				for cell in row.find_all('td'): # iterate through cells in the row
					if header_columns[index] == 'DATE':
						drow[header_columns[index]] = DT.strptime(cell.text, '%b %d, %Y').strftime('%Y-%m-%d')
					else:
						drow[header_columns[index]] = cell.text.replace("N/A", "") # add value to the dictionary

					if cell.find('a') != None:
						drow['opp_url'] = cell.find('a')['href']
					index += 1

				standing_list.append(drow) # add row to the list
			# print(standing_list)
			# print()

		elif title == "CLINCH STATISTICS":
			# get header labels to determine column counts and labels
			# this will allow you to scrap data without revising code 
			# even if the columns are changed in the future
			header_columns = []
			header = table.find('tr', class_='colhead').find_all('td')

			for column in header:
				header_columns.append(column.text.replace("%", "PERCENT"))

			# get statistics
			for row in table.find_all('tr', class_=['oddrow', 'evenrow']):# get rows of the table
				if len(row) == 0 or len(row) == 1:
					# print("No results for this statistics!")
					continue

				if len(row) != len(header_columns):
					# print("Warning: Columns mismatch!")
					continue

				# initialize variables repeatedly used
				index = 0
				drow = {}

				for cell in row.find_all('td'): # iterate through cells in the row
					if header_columns[index] == 'DATE':
						drow[header_columns[index]] = DT.strptime(cell.text, '%b %d, %Y').strftime('%Y-%m-%d')
					else:
						drow[header_columns[index]] = cell.text.replace("N/A", "") # add value to the dictionary
						
					if cell.find('a') != None:
						drow['opp_url'] = cell.find('a')['href']
					index += 1

				clinch_list.append(drow) # add row to the list
			# print(clinch_list)
			# print()
		elif title == "GROUND STATISTICS":
			# get header labels to determine column counts and labels
			# this will allow you to scrap data without revising code 
			# even if the columns are changed in the future
			header_columns = []
			header = table.find('tr', class_='colhead').find_all('td')

			for column in header:
				header_columns.append(column.text.replace("%", "PERCENT"))

			# get statistics
			for row in table.find_all('tr', class_=['oddrow', 'evenrow']):# get rows of the table
				if len(row) == 0 or len(row) == 1:
					# print("No results for this statistics!")
					continue

				if len(row) != len(header_columns):
					# print("Warning: Columns mismatch!")
					continue

				# initialize variables repeatedly used
				index = 0
				drow = {}

				for cell in row.find_all('td'): # iterate through cells in the row
					if header_columns[index] == 'DATE':
						drow[header_columns[index]] = DT.strptime(cell.text, '%b %d, %Y').strftime('%Y-%m-%d')
					else:
						drow[header_columns[index]] = cell.text.replace("N/A", "") # add value to the dictionary
						
					if cell.find('a') != None:
						drow['opp_url'] = cell.find('a')['href']
					index += 1

				ground_list.append(drow) # add row to the list
			# print(ground_list)
			# print()
		else:
			print("Unknown statistics! Skipping over.")
			continue

	return standing_list, clinch_list, ground_list


def read_db_and_write_to_excel():
	""" get necessary data from database and output into database
	param: None
	return: None
	"""

	# create a DB instance
	db = database.UFCHistoryDB('ufc_history.db')

	# get rows of information from all matches
	rows = db.get_rows_for_schema()

	# close database connection
	db.close_connection()

	write_to_excel(rows)

def fetch_information(start_id, url_list, key):
	""" iterate over url_list, send get request per each url, scrap data and write data into database
	param start_id: starting point of unique id of fighter on url_list
	param url_list: list of fighter urls 
	param key: first character of fighters' names
	"""

	# total number of fighter information from all threads
	# used to update progress bar
	global fetched_fighter_count

	# this bar represents total count from all threads
	global bar

	id_ = start_id

	tmp_list = []

	for furl in url_list:
		try:
			source = requests.get(get_page_url(furl, 'history')).text
		except Exception as e:
			print(f'Error(Main.request.get.history): {str(e)}')
			id_ += 1
			continue
		
		# get soup object
		soup = BeautifulSoup(source, 'lxml')
		# print(furl)

		ginfo = {}
		ginfo = get_general_info(soup)

		if len(ginfo) == 0:
			print(f'Cannot get general information from this url(F1): {furl}')
			print()
		
		ginfo['url'] = furl

		hinfo = get_history_info(soup)

		try:
			source = requests.get(get_page_url(furl, 'stats')).text
		except Exception as e:
			print(f'Error((F1)Main.request.get.stats): {str(e)}')
			id_ += 1
			continue
		
		# get soup object
		soup = BeautifulSoup(source, 'lxml')

		ss, cs, gs = get_statistics(soup)
		# hinfo, ss, cs, gs = None, None, None, None

		tmp_list.append((id_, ginfo, hinfo, ss, cs, gs))

		fetched_fighter_count += 1

		bar.update(fetched_fighter_count)

		id_ += 1

	# add fetched data from a thread into a list(global variable)
	info_list.append(tmp_list)

	# all threads are completed, insert data into database
	if len(info_list) >= total_thread_count:

		bar.finish()

		print('Writing fetched data into database...')

		db = database.UFCHistoryDB('ufc_history.db', True)

		db_bar = progressbar.ProgressBar(maxval=total_fighter_count, \
		widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(total_fighter_count)])
		db_bar.start()

		counter = 0

		# begin transaction on sqlite3 database
		# NOTE: this is important to optimize writing performance
		db.execute('BEGIN TRANSACTION')

		# bulk insert into database
		for info in info_list: # loop through information list fetched
			for item in info:
				db.insert_into_table_fighters(item[0], item[1])
				db.insert_into_table_history(item[0], item[2])
				db.insert_into_table_standing_stats(item[0], item[3])
				db.insert_into_table_clinch_stats(item[0], item[4])
				db.insert_into_table_ground_stats(item[0], item[5])

			# increase counter to update progress bar
			counter += 1

			# update progress bar
			db_bar.update(counter)

		db_bar.finish()

		# commit all pending insert queries
		db.execute('COMMIT')

		print('Writing fetched data into database is completed!')

		if work_mode == 0:
			db.get_rows_for_schema()

		print('Done!')

def signal_handler(sig, frame):
	""" Signal handler
		This will prevent to show complicated text of exceptions on keyboard interrupt
	param sig: signal identifier
	param frame:
	return:
	"""

	print(f'SIGNAL {sig} CAUGHT.')
	print('End the process according to request.')
	sys.exit()

def parse_args(argv):
	""" main function to handle argument parsing and do actual work
	param argv: list of argument
	return: mode as a single digit
	"""

	# value 0: default mode | scrap >> write_to_database >> output to excel
	# value 1: scrap >> write_to_database
	# value 2: output to excel based on already existing databse
	mode = 0

	try:
		opts, args = getopt.getopt(argv,"hm:", ["mode="])
	except getopt.GetoptError:
		print('Argument Error: python main.py -m <number>')
		sys.exit()

	for opt, arg in opts:
		if opt == '-h':
			print('python main.py -m <number>')
			print('Mode 0: default mode | scrap >> write_to_database >> output to excel')
			print('Mode 1: scrap >> write_to_database')
			print('Mode 2: output to excel based on already existing databse')
			sys.exit(2)
		elif opt in ("-m", "--mode"):
			mode = int(arg)

	if mode not in range(0, 3):
		print('Argument Error: Mode should be in range 0 ~ 2')
		print('Mode 0: default mode | scrap >> write_to_database >> output to excel')
		print('Mode 1: scrap >> write_to_database')
		print('Mode 2: output to excel based on already existing databse')
		sys.exit()

	return mode

if __name__ == "__main__":

	""" represents how the script should work
	value True: Just scrap and write to database without excel output
	value False: scrap, write to db and make excel output
	"""
	global work_mode

	work_mode = parse_args(sys.argv[1:])
	
	signal.signal(signal.SIGINT, signal_handler)

	if work_mode == 2:
		db = database.UFCHistoryDB('ufc_history.db')
		db.get_rows_for_schema()
	else:

		fighter_urls = {}

		search_keys = list(string.ascii_lowercase)

		print("Fetching urls of fighters...")

		count_list = []

		# global variable for total count of fighters
		global total_fighter_count
		total_fighter_count = 0

		# this is used to update progress bar for scraping
		global fetched_fighter_count
		fetched_fighter_count = 0

		# represents total count of threads to scrap
		global total_thread_count
		total_thread_count = 0

		""" defines how to split list into threads
		value 'alphabet': all urls of fighters whose name starts with a unique alphabet goes into a single thread
		value 'specify_count_per_thread': every thread contains only specified number of urls
		"""

		thread_distribution_mode = 'specify_count_per_thread'

		# represents how many urls are charged for each thread
		count_per_thread = 50

		# list of urls of fighters
		all_url_list = []

		# this progress bar is used to show the progress of fetching urls of all fighters
		url_bar = progressbar.ProgressBar(maxval=len(search_keys), \
			widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(search_keys))])
		url_bar.start()

		# this mode does create 26 threads for each alphabet
		if thread_distribution_mode == 'alphabet':

			try:
				for index, key in enumerate(search_keys):
					url_list = get_fighter_url_list_startwith(key)
					fighter_urls[key] = url_list
					count_list.append(len(url_list))
					total_fighter_count += len(url_list)
					total_thread_count = len(fighter_urls)
					url_bar.update(index + 1)

			except Exception as e:

				print(f'Failed to fetch urls due to error: {str(e)}')
				exit()
		# this mode do multithreading to speed up scraping
		elif thread_distribution_mode == 'specify_count_per_thread':
			try:
				for index, key in enumerate(search_keys):

					url_list = get_fighter_url_list_startwith(key)
					all_url_list += url_list
					total_fighter_count += len(url_list)
					url_bar.update(index + 1)

				# divide all_url_list into smaller lists which contain 'count_per_thread' number of fighter urls maximum
				tmp_list = [all_url_list[x:x + count_per_thread] for x in range(0, len(all_url_list), count_per_thread)]

				all_url_list.clear()

				all_url_list = tmp_list

				# get required thread count
				total_thread_count = len(all_url_list)

			except Exception as e:

				print(f'Failed to fetch urls due to error: {str(e)}')
				url_bar.finish()
				exit()
		else:
			print('Unknown thread_distribution_mode.')
			url_bar.finish()
			exit()

		url_bar.finish()

		print(f'Fetched {total_fighter_count} urls in total!')

		key_index = 0

		print("Scraping information...")

		# list that contains all information fetched
		global info_list
		info_list = []

		# this progress bar shows the progress of scraping informations of fighters, history, statistics ...
		global bar

		bar = progressbar.ProgressBar(maxval=total_fighter_count, \
			widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(total_fighter_count)])
		bar.start()

		# create threads that do actual scraping
		if thread_distribution_mode == 'alphabet':

			for key in search_keys:

				thread_ = threading.Thread(target=fetch_information, args=(sum(count_list[:key_index]) + 1, fighter_urls[key], key))

				key_index += 1

				thread_.start()
		elif thread_distribution_mode == 'specify_count_per_thread':

			for index, list_ in enumerate(all_url_list):

				thread_ = threading.Thread(target=fetch_information, args=(count_per_thread * index + 1, list_, str(index + 1)))

				thread_.start()
	