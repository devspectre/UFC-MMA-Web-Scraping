import requests
import string
from bs4 import BeautifulSoup
from openpyxl import Workbook

def list_to_string(list, delimiter):
	""" returns a string from given list joined with given delimiter
	"""

	return f'{delimiter}'.join(str(element) for element in list)

def get_page_url(url, page_name):
	""" this function retrieve url of fighter's stat page from profile url."""
	
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
	group = None

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
			group = item.text
	except Exception as e:
		pass

	try:
		meta_data = soup.find('ul', class_='player-metadata')
		# print(meta_data)
		for li in meta_data.find_all('li'):
			try:
				span = li.find('span', text='Birth Date')
				try:
					age = li.text.split(":")[1].split(")")[0].strip()
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
	info_list['group'] = group
	# print(f'Name={name}, WC={weight_class}, Height={height}, Weight={weight}, Age={age}, Reach={reach}, Group={group}')
	# print()

	return info_list

def get_history_info(soup):
	""" returns a list of sub lists
		each sub list contains match _date, event, opponent, result, decision, rounds and time
	"""

	# get fight history information from the table
	#
	# !NOTE: ensure there's only one table body on the history page

	tbody = soup.find('table', class_='tablehead mod-player-stats')

	if tbody is None:
		print("Cannot find table on the page")
		return info_list

	history_list = []

	for row in tbody.find_all('tr', class_=['oddrow', 'evenrow']):
		cells = row.find_all('td')
		if len(cells) == 0 or len(cells) == 1:
			print("No item in the row!")
			continue
		else:
			match_date = cells[0].text
			event = cells[1].text
			opponent = cells[2].text
			result = cells[3].text
			decision = cells[4].text
			rounds = cells[5].text
			time = cells[6].text
			history_list.append([match_date, event, opponent, result, decision, rounds, time])

	return history_list

if __name__ == "__main__":

	all_list = {}
	# search_indices = list(string.ascii_lowercase)

	# for index in search_indices:
		# all_list[index] = get_fighter_url_list_startwith(index)

	all_list['a'] = get_fighter_url_list_startwith('a')

	for furl in all_list['a']:
		source = requests.get(get_page_url(furl, 'history')).text
		# get soup object
		soup = BeautifulSoup(source, 'lxml')
		print(furl)
		print(get_general_info(soup))
		print(get_history_info(soup))
		print()
		

	print('Done!')