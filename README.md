# mma_scraper
Get rich history of mma fights

# Dependency
Python: 3
Modules: BeautifulSoup, requests, sqlite3, xlsxwriter, string, threading, datetime, signal, progressbar

`pip install -r requirements.txt
`

# Run
Make sure you've installed those dependencies.
Run the script in command line.

- default mode:
python main.py

python main.py -m <mode_number>


mode_number:
            
            0: default mode | scrap >> write_to_database >> output to excel
            
            1: scrap >> write_to_database'
            
            2: output to excel based on already existing database, test purpose

