import requests
import string
import progressbar
from bs4 import BeautifulSoup
from datetime import datetime as DT

import database

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
		print("Cannot find general info on the page!")
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
		return info_list

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

def get_standing_stats(soup):
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

			index = 0
			drow = {}
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
				drow.clear()

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

			index = 0
			drow = {}
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
				drow.clear()

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

			index = 0
			drow = {}
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
				drow.clear()

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


if __name__ == "__main__":

	all_list = {}
	search_indices = list(string.ascii_lowercase)

	# instance to manage sqlite3 database
	db = database.UFCHistoryDB('ufc_history.db')

	print("Fetching urls of fighters...")

	total_fighter_count = 0

	for index in search_indices:
		url_list = get_fighter_url_list_startwith(index)
		all_list[index] = url_list
		total_fighter_count += len(url_list)

	print(f'Fetched {total_fighter_count} urls in total!')

	# all_list['a'] = get_fighter_url_list_startwith('a')

	print("Fetching fighters' urls done!")

	id_ = 0
	count_per_alpha = 0
	limit_per_alpha = 30

	print("Scraping information...")

	bar = progressbar.ProgressBar(maxval=26*limit_per_alpha, \
		widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
	bar.start()

	for index in search_indices:
		count_per_alpha = 0
		# print(f'---------------------- < {index} > ----------------------')
		for furl in all_list[index]:
			source = requests.get(get_page_url(furl, 'history')).text
			# get soup object
			soup = BeautifulSoup(source, 'lxml')
			# print(furl)
			ginfo = get_general_info(soup)
			ginfo['url'] = furl
			db.insert_into_table_fighters(id_, ginfo)

			hinfo = get_history_info(soup)
			db.insert_into_table_history(id_, hinfo)

			source = requests.get(get_page_url(furl, 'stats')).text
			# get soup object
			soup = BeautifulSoup(source, 'lxml')

			ss, cs, gs = get_standing_stats(soup)
			db.insert_into_table_standing_stats(id_, ss)
			db.insert_into_table_clinch_stats(id_, cs)
			db.insert_into_table_ground_stats(id_, gs)

			bar.update(id_ + 1)

			id_ += 1

			count_per_alpha += 1

			if count_per_alpha >= limit_per_alpha:
				break

			# print()

		# print()
	bar.finish()
		
	db.close_connection()

	print('Done!')