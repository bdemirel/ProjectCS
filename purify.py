"""
    python3 purify.py -y YYYY
    @param YYYY	= year in 4 digits, e.g: 2018
"""
import os, logging, sys, getopt, sqlite3, getpass, tldextract
from sqlite3 import Error

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
	opts, args = getopt.getopt(sys.argv[1:], "hy:")
except getopt.GetoptError as err:
	logger.error(str(err))
	sys.exit(1)

for opt, val in opts:
	if opt == "-h":
		print("p3.py -y <year-to-parse>")
		sys.exit()
	elif opt == "-y":
		parseyear = int (val)

if not 'parseyear' in locals():
	logger.info("Missing arguments!")
	sys.exit(1)

#DB Location (!!!Change according to device)
dbloc = os.path.join("/data", getpass.getuser(), "{}.db".format(getpass.getuser()))
newloc = os.path.join("/data", getpass.getuser(), "consistent.db")

def callback(stmt):
	logger.debug(stmt)

def trimDomain(domain):
	extract = tldextract.extract(domain)
	return "{}.{}".format(extract[1], extract[2])

try:
	oldConn = sqlite3.connect(dbloc)
	newConn = sqlite3.connect(newloc)
except Error as err:
	logger.error("Cannot connect to database!")
	logger.error(err)
	sys.exit(1)
sqlite3.enable_callback_tracebacks(True)
oldConn.row_factory = sqlite3.Row
newConn.row_factory = sqlite3.Row
newConn.create_function("domain", 1, trimDomain)
logger.info("Database Connection started")
#conn.set_trace_callback(callback)

stmtFetch = "SELECT * FROM `{0}` WHERE (query_name, query_type) IN (SELECT query_name, query_type FROM `{0}` GROUP BY query_name, query_type HAVING COUNT (DISTINCT parsedate) = 24)".format(parseyear)
stmtCheck = "SELECT rowid, * FROM `{0}` WHERE query_name = ? AND query_type = ? AND parsedate = ?".format(parseyear)
stmtPush = "INSERT INTO `{0}` (query_name, query_type, response_name, response_type, cname, dname, parsedate, timestamp, ipv4, ipv6, as_full, country, cdn) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(parseyear)
stmtRepush = "UPDATE `{0}` SET response_name = ?, response_type = ?, cname = ?, dname = ?, timestamp = ?, ipv4 = ?, ipv6 = ?, as_full = ?, country = ?, cdn = ? WHERE rowid = ?".format(parseyear)
stmtParse = "INSERT INTO `cdn{0}` SELECT domain(cname), COUNT(*) FROM `{0}` WHERE cdn = 1 GROUP BY domain(cname) ORDER BY COUNT(*) DESC".format(parseyear)

#	@param	row		= from the old db to be put into new db
#	@param	checker	= already in the new db
for row in oldConn.execute(stmtFetch):
	logger.debug("i")
	checker = newConn.execute(stmtCheck, (row['query_name'], row['query_type'], row['parsedate'])).fetchone()
	if checker == None:
		if row['response_type'] != "CNAME" and trimDomain(row['response_name']) != trimDomain(row['query_name']):
			newConn.execute(stmtPush, (row['query_name'], row['query_type'], row['response_name'], row['response_type'], row['response_name'], row['dname'], row['parsedate'], row['timestamp'], row['ipv4'], row['ipv6'], row['as_full'], row['country'], 1))
		else:
			newConn.execute(stmtPush, (row['query_name'], row['query_type'], row['response_name'], row['response_type'], row['cname'], row['dname'], row['parsedate'], row['timestamp'], row['ipv4'], row['ipv6'], row['as_full'], row['country'], row['cdn']))		
	else:
		if checker['response_type'] == "CNAME" and row['response_type'] != "CNAME":
			if trimDomain(row['response_name']) != trimDomain(row['query_name']):
				newConn.execute(stmtRepush, (row['response_name'], row['response_type'], row['response_name'], row['dname'], row['timestamp'], row['ipv4'], row['ipv6'], row['as_full'], row['country'], 1, checker['rowid']))
				""" elif checker['cdn'] == 1:
				row['cname'] = checker['cname']
				row['cdn'] = checker['cdn'] """
			else:
				newConn.execute(stmtRepush, (row['response_name'], row['response_type'], row['cname'], row['dname'], row['timestamp'], row['ipv4'], row['ipv6'], row['as_full'], row['country'], row['cdn'], checker['rowid']))
		elif checker['response_type'] == "CNAME" and row['response_type'] == "CNAME" and checker['cdn'] == 0:
			if trimDomain(row['response_name']) != trimDomain(row['query_name']):
				newConn.execute(stmtRepush, (row['response_name'], row['response_type'], row['response_name'], row['dname'], row['timestamp'], row['ipv4'], row['ipv6'], row['as_full'], row['country'], 1, checker['rowid']))
		else:
			logger.debug("OOPS")
			logger.debug(str(checker))
			logger.debug(str(row))

newConn.commit()
logger.info("Table {} committed".format(parseyear))
newConn.execute(stmtParse)
newConn.commit()
logger.info("Table cdn{} committed")
