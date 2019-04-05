import mysql.connector as DBCon, logging, sys, glob, getpass, json, getopt, subprocess, itertools, os
from mysql.connector import pooling
from mysql.connector import Error
from multiprocessing import Pool
from itertools import zip_longest

#logging
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = logging.FileHandler('p2.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)-8s %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)

#start database pool
try:
	global dbpool
	dbpool = DBCon.pooling.MySQLConnectionPool(pool_name="dbpool", pool_size=7, option_files='mysql_options.cnf')	
except:
	logger.info("Cannot connect to MySQL!")
	#logger.info(err)
	sys.exit(1)
logger.info("DB Connection Pool started")

def parse(dyear, jsonfile):
	logger.info("File started: %s", jsonfile)
	conn = dbpool.get_connection()
	print(conn.is_connected())
	if (not conn.is_connected()):
		return(0, 0)
	else:
		cursor = conn.cursor(prepared=True)
		
		stmtCDN = "INSERT INTO cdn"+str(dyear)+" (`domain`, `count`) VALUES (?, ?)"
		cursor.execute(stmtCDN)
		stmtAll = "INSERT INTO `"+str(dyear)+"` (query_name, query_type, response_name, response_type, cname, dname, timestamp, ipv4, ipv6, as_full, country, cdn) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		cursor.execute(stmtAll)
		with open(jsonfile) as jf:
			contents = json.load(jf)
			for row in contents:
				if "_id" in row:
					cursor.execute(stmtAll, (row["query_name"], row["query_type"], row["response_name"], row["response_type"], row["cname_name"], row["dname_name"], row["timestamp"]/1000, row["ip4_address"], row["ip6_address"], row["as_full"], row["country"], row["_cdn"]))
				elif "dname_count" in row:
					dcount = row["dname_count"]
					ccount = row["cname_count"]
				else:
					for site, cnt in row.items():
						cursor.execute(stmtCDN, (site, cnt))
		conn.close()
		jf.close()
		os.remove(jsonfile)
		return (dcount, ccount)

def main():
	#parse commandline arguments
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hoy:")
	except getopt.GetoptError as err:
		logger.info(str(err))
		sys.exit(1)
	
	dataset = "alexa1m"
	for opt, val in opts:
		if opt == "-h":
			print("p2.py (-o) -y <year-to-parse>")
			sys.exit()
		elif opt == "-o":
			dataset = "open-tld"
		elif opt == "-y":
			parseyear = int (val)
	if not 'parseyear' in locals():
		logger.info("Missing year!")
		sys.exit(1)

	#set arguments, start and run multiprocessing pool
	pool = Pool(processes=7, maxtasksperchild=1)
	subprocess.run(['./decompression.sh', str(parseyear), dataset], check=True)
	results = []
	dcount = ccount = 0
	while True:
		batch = glob.glob(os.path.join("/data", getpass.getuser(), "results", dataset, "parse", "*.json"))[:7]
		if batch == []:
			break
		arglist = [(parseyear, x) for x in batch]
		results += pool.starmap(parse, arglist, 1)
		subprocess.run(['./decompression.sh', str(parseyear), dataset], check=True)
		sys.exit(0)
	"""for batch in batcher(glob.glob(os.path.join("/data", getpass.getuser(), "results", dataset, "parse", "*.json")), 12):
		arglist = []
		for file in batch:
			arglist.append((parseyear, file))
		pool.starmap(parse, arglist, 1)
		subprocess.run(['./decompression.sh', str(parseyear), dataset], check=True)"""
	for dnames, cnames in results:
		dcount += dnames
		ccount += cnames
	logger.info("Domain names imported: %s", dcount)
	logger.info("CDN domains imported: %s", ccount)

if __name__ == "__main__":
	main()