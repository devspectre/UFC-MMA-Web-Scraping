import requests
import string
from queue import Queue
import threading
import signal
import progressbar
from bs4 import BeautifulSoup
from datetime import datetime as DT

import database
from excel import ExcelWriter

# global variable for total count of fighters
total_fighter_count = 0

# global variable for progressbar
bar = None

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

	info_list['name'] = name
	info_list['age'] = age
	info_list['reach'] = reach
	info_list['weight_class'] = weight_class
	info_list['height'] = height
	info_list['weight'] = weight
	info_list['group_name'] = group_name
	# print(f'Name={name}, WC={weight_class}, Height={height}, Weight={weight}, Age={age}, Reach={reach}, Group={group}')
	# print()

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

def fetch_information(start_id, url_list, key):
	""" iterate over url_list, send get request per each url, scrap data and write data into database
	param start_id: starting point of unique id of fighter on url_list
	param url_list: list of fighter urls 
	param key: first character of fighters' names
	"""

	print(key, start_id, url_list[0])

	id_ = start_id

	bar = progressbar.ProgressBar(maxval=len(url_list), \
		widgets=[key, progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' | ', progressbar.Counter(), '/', str(len(url_list))])
	bar.start()

	for furl in url_list:
		try:
			source = requests.get(get_page_url(furl, 'history')).text
		except Exception as e:
			print(f'Error(Main.request.get.history): {str(e)}')
			id_ += 1
			total_fighter_count -= 1
			continue
		
		# get soup object
		soup = BeautifulSoup(source, 'lxml')
		# print(furl)
		ginfo = get_general_info(soup)

		if len(ginfo) == 0:
			print(furl)
			print('Cannot get general information from this url')

		ginfo['url'] = furl
		# db.insert_into_table_fighters(id_, ginfo)

		hinfo = get_history_info(soup)
		# db.insert_into_table_history(id_, hinfo)

		try:
			source = requests.get(get_page_url(furl, 'stats')).text
		except Exception as e:
			print(f'Error(Main.request.get.stats): {str(e)}')
			id_ += 1
			total_fighter_count -= 1
			continue
		
		# get soup object
		soup = BeautifulSoup(source, 'lxml')

		ss, cs, gs = get_statistics(soup)
		# db.insert_into_table_standing_stats(id_, ss)
		# db.insert_into_table_clinch_stats(id_, cs)
		# db.insert_into_table_ground_stats(id_, gs)

		queue_entry = [id_, ginfo, hinfo, ss, cs, gs]
		q.put(queue_entry)

		bar.update(id_ - start_id + 1)

		id_ += 1

	if id_ - start_id >= len(url_list):
		bar.finish()

def read_db_and_write_to_excel():
	""" 
	"""

	# create a DB instance
	db = database.UFCHistoryDB('ufc_history.db')

	# get rows of information from all matches
	rows = db.get_rows_for_schema()

	# close database connection
	db.close_connection()

	# create an excel writer instance
	xw = ExcelWriter('ufc_history')
	
	# create horizontal header
	xw.set_header_list(header_list)

	# initialize variables row_index, our default template starts from row 4
	row_index = 4

	# write to excel file
	# NOTE: Using dictionary rather than list makes it easier to change/revise and maintain
	# can easily understand what is what
	for row in rows:
		xw.write_to_sheet(row_index, 0, row['Date'])
		xw.write_to_sheet(row_index, 1, row['WeightClass'])
		xw.write_to_sheet(row_index, 2, row['Winner'])
		xw.write_to_sheet(row_index, 3, row['DecisionType'])
		xw.write_to_sheet(row_index, 4, row['Rounds'])
		xw.write_to_sheet(row_index, 5, row['Time'])
		xw.write_to_sheet(row_index, 6, row['IsTitle?'])

		xw.write_to_sheet(row_index, 7, row['F1Name'])
		xw.write_to_sheet(row_index, 8, row['F1Height'])
		xw.write_to_sheet(row_index, 9, row['F1Reach'])
		xw.write_to_sheet(row_index, 10, row['F1Age'])

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

		xw.write_to_sheet(row_index, 7, row['F2Name'])
		xw.write_to_sheet(row_index, 8, row['F2Height'])
		xw.write_to_sheet(row_index, 9, row['F2Reach'])
		xw.write_to_sheet(row_index, 10, row['F2Age'])

		xw.write_to_sheet(row_index, 11, row['F2SDBL'])
		xw.write_to_sheet(row_index, 12, row['F2SDBA'])
		xw.write_to_sheet(row_index, 13, row['F2SDHL'])
		xw.write_to_sheet(row_index, 14, row['F2SDHA'])
		xw.write_to_sheet(row_index, 15, row['F2SDLL'])
		xw.write_to_sheet(row_index, 16, row['F2SDLA'])
		xw.write_to_sheet(row_index, 17, row['F2TSL'])
		xw.write_to_sheet(row_index, 18, row['F2TSA'])
		xw.write_to_sheet(row_index, 19, row['F2SSL'])
		xw.write_to_sheet(row_index, 20, row['F2SSA'])
		xw.write_to_sheet(row_index, 21, row['F2SA'])
		xw.write_to_sheet(row_index, 22, row['F2KD'])

		xw.write_to_sheet(row_index, 23, row['F2SCBL'])
		xw.write_to_sheet(row_index, 24, row['F2SCBA'])
		xw.write_to_sheet(row_index, 25, row['F2SCHL'])
		xw.write_to_sheet(row_index, 26, row['F2SCHA'])
		xw.write_to_sheet(row_index, 27, row['F2SCLL'])
		xw.write_to_sheet(row_index, 28, row['F2SCLA'])
		xw.write_to_sheet(row_index, 29, row['F2RV'])
		xw.write_to_sheet(row_index, 30, row['F2SR'])
		xw.write_to_sheet(row_index, 31, row['F2TDL'])
		xw.write_to_sheet(row_index, 32, row['F2TDA'])
		xw.write_to_sheet(row_index, 33, row['F2TDS'])

		xw.write_to_sheet(row_index, 34, row['F2SGBL'])
		xw.write_to_sheet(row_index, 35, row['F2SGBA'])
		xw.write_to_sheet(row_index, 36, row['F2SGHL'])
		xw.write_to_sheet(row_index, 37, row['F2SGHA'])
		xw.write_to_sheet(row_index, 38, row['F2SGLL'])
		xw.write_to_sheet(row_index, 39, row['F2SGLA'])
		xw.write_to_sheet(row_index, 40, row['F2AD'])
		xw.write_to_sheet(row_index, 41, row['F2ADTB'])
		xw.write_to_sheet(row_index, 42, row['F2ADHG'])
		xw.write_to_sheet(row_index, 43, row['F2ADTM'])
		xw.write_to_sheet(row_index, 44, row['F2ADTS'])
		xw.write_to_sheet(row_index, 45, row['F2SM'])

		row_index += 1

	# save the file and close safely
	xw.done()


# global variable for queue
q = Queue()

def write_to_db():
	""" writes queued data into database
	return: 
	"""

	# instance to manage sqlite3 database
	db = database.UFCHistoryDB('ufc_history.db', True)

	counter = 0

	while True:
		entry = q.get()
		if entry is None:
			break
		db.insert_into_table_fighters(entry[0], entry[1])
		db.insert_into_table_history(entry[0], entry[2])
		db.insert_into_table_standing_stats(entry[0], entry[3])
		db.insert_into_table_clinch_stats(entry[0], entry[4])
		db.insert_into_table_ground_stats(entry[0], entry[5])
		print(f'{counter} rows written')
		q.task_done()
		counter += 1
		if counter >= total_fighter_count:
			break

	db.close_connection()
	read_db_and_write_to_excel()
	print('Done!')

if __name__ == "__main__":

	all_list = {}
	search_keys = list(string.ascii_lowercase)

	print("Fetching urls of fighters...")

	count_list = []

	for key in search_keys:
		url_list = get_fighter_url_list_startwith(key)
		all_list[key] = url_list
		count_list.append(len(url_list))
		total_fighter_count += len(url_list)

	print(f'Fetched {total_fighter_count} urls in total!')

	# all_list['a'] = get_fighter_url_list_startwith('a')

	print("Fetching fighters' urls done!")

	key_index = 0
	# count_per_alpha = 0
	# limit_per_alpha = 30

	print("Scraping information...")

	db_thread = threading.Thread(target=write_to_db)
	db_thread.start()

	for key in search_keys:

		thread_ = threading.Thread(target=fetch_information, args=(sum(count_list[:key_index + 1]) + 1, all_list[key], key))

		key_index += 1

		thread_.start()
		
	