import xlsxwriter
from xlsxwriter import Workbook
import sqlite3

class ExcelWriter:
	""" writes data to excel sheet and save it into a file

	"""

	# create a global variable of header list
	header_list = [
		'Date',
		'WeightClass',
		'Winner',
		'DecisionType',
		'Rounds',
		'Time',
		'IsTitle?',
		'F1Name',
		'F1Height',
		'F1Reach',
		'F1Age',

		'F1SDBL',
		'F1SDBA',
		'F1SDHL',
		'F1SDHA',
		'F1SDLL',
		'F1SDLA',
		'F1TSL',
		'F1TSA',
		'F1SSL',
		'F1SSA',
		'F1SA',
		'F1KD',

		'F1SCBL',
		'F1SCBA',
		'F1SCHL',
		'F1SCHA',
		'F1SCLl',
		'F1SCLA',
		'F1RV',
		'F1SR',
		'F1TDL',
		'F1TDA',
		'F1TDS',

		'F1SGBL',
		'F1SGBA',
		'F1SGHL',
		'F1SGHA',
		'F1SGLL',
		'F1SGLA',
		'F1AD',
		'F1ADTB',
		'F1ADHG',
		'F1ADTM',
		'F1ADTS',
		'F1SM',

		'F2Name',
		'F2Height',
		'F2Reach',
		'F2Age',

		'F2SDBL',
		'F2SDBA',
		'F2SDHL',
		'F2SDHA',
		'F2SDLL',
		'F2SDLA',
		'F2TSL',
		'F2TSA',
		'F2SSL',
		'F2SSA',
		'F2SA',
		'F2KD',

		'F2SCBL',
		'F2SCBA',
		'F2SCHL',
		'F2SCHA',
		'F2SCLl',
		'F2SCLA',
		'F2RV',
		'F2SR',
		'F2TDL',
		'F2TDA',
		'F2TDS',

		'F2SGBL',
		'F2SGBA',
		'F2SGHL',
		'F2SGHA',
		'F2SGLL',
		'F2SGLA',
		'F2AD',
		'F2ADTB',
		'F2ADHG',
		'F2ADTM',
		'F2ADTS',
		'F2SM'
	]

	def __init__(self, xl_file: str):
		""" constructor
		:param xl_file: excel file name
		"""

		# file name
		self.file_name = xl_file

		# create a workbook for instance
		self.wb = Workbook(f'{xl_file}.xlsx')

		# create a new sheet for the workbook
		self.sheet = self.wb.add_worksheet('Sheet 1')

	def set_header_list(self, h_list: list):
		""" set the header list of instance
		:param h_list: list of header labels !NOTE: make sure this matches with actual data
		:return:
		"""

		# create a format for merged cell
		merge_format = self.wb.add_format({
			'bold': 1,
			'align': 'center',
			'valign': 'vcenter',
			})

		try:
			# create and write merged cell
			self.sheet.merge_range('A3:G3', 'General Info (From Fight History Page)', merge_format)
			self.sheet.merge_range('H3:K3', 'F1 Fighter Info', merge_format)
			self.sheet.merge_range('L3:W3', 'F1 Striking Stats', merge_format)
			self.sheet.merge_range('X3:AH3', 'F1 Clinch Stats', merge_format)
			self.sheet.merge_range('AI3:AT3', 'F1 Ground Stats', merge_format)
			self.sheet.merge_range('AU3:AX3', 'F2 Fighter Info', merge_format)
			self.sheet.merge_range('AY3:BJ3', 'F2 Striking Stats', merge_format)
			self.sheet.merge_range('BK3:BU3', 'F2 Clinch Stats', merge_format)
			self.sheet.merge_range('BV3:CG3', 'F2 Ground Stats', merge_format)
		except Exception as e:
			print(f'Error(Excel.set_header_list): {str(e)}')

		col = 0

		# iterate over the header labels and write it out column by column
		for label in h_list:
			self.sheet.write(3, col, label)
			col += 1

	def write_to_sheet(self, row_id: int, col_id: int, value: str):
		""" write 'value' to the sheet at (row_id, col_id)
		:param row_id: index of row on the sheet
		:param col_id: index of column on the sheet
		:param value: actual data to be written on the sheet
		:return: true if successful or false in case of failure
		"""

		try:
			self.sheet.write(row_id, col_id, value)
			return True
		except Exception as e:
			print(f'Error(Excel.write_to_sheet): {str(e)}')
			return False

	# def write_to_file(self, file_name='xlsx_temp'):
	# 	""" write the sheet(s) into a file named 'file_name'
	# 	param file_name: file name to be written on the disk
	# 	return: true if successful or false in case of failure
	# 	"""

	# 	try:
	# 		self.wb.save(f'{file_name}.xls')
	# 		return True				
	# 	except Exception as e:
	# 		print(f'Error(Excel.write_to_file): {str(e)}')
	# 		return False

	def done(self):
		self.wb.close()
		
if __name__ == '__main__':

	xw = ExcelWriter('xlwt_example')
	xw.set_header_list(xw.header_list)
	xw.done()
	print('Created a temporary excel file with header only.')
	print('Run the main script to scrap data.')