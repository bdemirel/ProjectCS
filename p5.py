"""
    python3 p5.py -y YYYY -v A(AAA) -p n
    @param YYYY	= year in 4 digits, e.g: 2018
	@param A(AAA) = DNS query type for IP version, e.g: A for IPv4 and AAAA for IPv6
	@param n	= number of processes, ideally should be a divisor of 24, e.g: 6, 8 or 12
"""
import os, logging, sys, getopt, sqlite3, getpass, time, tldextract, json
from sqlite3 import Error
from multiprocessing import Pool

#logging (only console)
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)-8s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#parse commandline arguments
try:
	opts, args = getopt.getopt(sys.argv[1:], "hy:v:p:")
except getopt.GetoptError as err:
	logger.error(str(err))
	sys.exit(1)

for opt, val in opts:
	if opt == "-h":
		print("p3.py -y <year-to-parse>")
		sys.exit()
	elif opt == "-y":
		parseyear = int (val)
	elif opt == "-v":
		ipv = val
	elif opt == "-p":
		concurrency = int (val)

if not all(argument in globals() for argument in ['parseyear', 'ipv', 'concurrency']):
	logger.info("Missing arguments!")
	sys.exit(1)

#DB Location (!!!Change according to device)
dbloc = os.path.join("/data", getpass.getuser(), "{}.db".format(getpass.getuser()))

def callback(stmt):
	logger.debug(stmt)

def cdn(cname, flag):
	if flag:
		extract = tldextract.extract(cname)
		return "{}.{}".format(extract[1], extract[2])
	else:
		return "None"

def query(date):
	try:
		conn = sqlite3.connect(dbloc)
	except Error as err:
		logger.error("Cannot connect to database!")
		logger.error(err)
		sys.exit(1)
	cursor = conn.cursor()
	logger.info("Database Connection started")
	conn.create_function("cdnDomain", 2, cdn)
	#conn.row_factory = sqlite3.Row
	#conn.set_trace_callback(callback)

	stmt = "SELECT cdnDomain(cname, cdn) FROM `{}` WHERE parsedate = ? AND query_type = ? ORDER BY rowid ASC".format(parseyear)
	logger.debug("Execute {}".format(date))
	cursor.execute(stmt, (date, ipv))
	results = cursor.fetchall()
	results = [rows[0] for rows in results]
	results.insert(0, date)
	return results

def main():
	dates = ["{}{:02d}{}".format(parseyear, y, x) for y in range(1,13) for x in ["01", "15"]]

	pool = Pool(concurrency)
	res = pool.map(query, dates, 1)
	#print(res)
	with open('results.out', 'a') as file:
		file.write("{} ------ [p5.py] ------\n".format(time.asctime()))
		file.write(json.dumps(res))
		file.write("\n\n")

if __name__ == "__main__":
	main()