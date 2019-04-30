"""
    python3 p5.py -y YYYY -p n
    @param YYYY	= year in 4 digits, e.g: 2018
	@param n	= number of processes, ideally should be a divisor of 24, e.g: 6, 8 or 12
"""
import os, logging, sys, getopt, sqlite3, getpass
from sqlite3 import Error
from multiprocessing import Pool

#logging (only console)
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)-8s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#parse commandline arguments
try:
	opts, args = getopt.getopt(sys.argv[1:], "hy:p:")
except getopt.GetoptError as err:
	logger.error(str(err))
	sys.exit(1)

for opt, val in opts:
	if opt == "-h":
		print("p3.py -y <year-to-parse>")
		sys.exit()
	elif opt == "-y":
		parseyear = int (val)
	elif opt == "-p":
		concurrency = int (val)

if not ('parseyear' in locals() and 'concurrency' in locals()):
	logger.info("Missing arguments!")
	sys.exit(1)

#DB Location (!!!Change according to device)
dbloc = "/home/bd/dragon/data/bdemirel/bdemirel.db"

def callback(stmt):
	logger.debug(stmt)

def query(date):
	try:
		conn = sqlite3.connect(dbloc)
	except Error as err:
		logger.error("Cannot connect to database!")
		logger.error(err)
		sys.exit(1)
	cursor = conn.cursor()
	logger.info("Database Connection started")
	conn.set_trace_callback(callback)

	results = []
	stmts = [
		"SELECT COUNT(a.rowid), query_name FROM `{0}` a INNER JOIN cdn{0} c ON a.cname = c.domain WHERE a.parsedate = ? GROUP BY a.cname".format(parseyear),
		"SELECT COUNT(*) FROM `{0}` WHERE parsedate = ? AND cdn = 0".format(parseyear)
	]
	for s in stmts:
		logger.debug("Execute {}".format(date))
		cursor.execute(s, (date,))
		results += cursor.fetchall()
	return results

def main():
	dates = ["{}{:02d}{}".format(parseyear, y, x) for y in range(1,13) for x in ["01", "15"]]

	pool = Pool(concurrency)
	res = pool.map(query, dates, 1)
	print(res)

if __name__ == "__main__":
	main()