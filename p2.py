import mysql.connector as DBCon, logging, sys, glob, getpass, json, getopt, subprocess, itertools, os
from mysql.connector import pooling
from multiprocessing import Pool

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

#from: https://stackoverflow.com/questions/28022223/how-to-iterate-over-a-dictionary-n-key-value-pairs-at-a-time by @georg
def batcher(it, size):
    it = iter(it)
    while True:
        p = tuple(itertools.islice(it, size))
        if not p:
            break
        yield p

def parse(dyear, conn, jsonfile):
	logger.info("File started: %s", jsonfile)
	cursor = conn.cursor(prepared=True)
	stmtCDN = "INSERT INTO cdn"+dyear+" (`domain`, `count`) VALUES (?, ?)"
	cursor.execute(stmtCDN)
	stmtAll = "INSERT INTO "+dyear+" (query_name, query_type, response_name, response_type, cname, dname, timestamp, ipv4, ipv6, as, country, cdn) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	cursor.execute(stmtAll)
	with open(jsonfile) as jf:
		contents = json.load(jf)
		for row in contents:
			if row["_id"]:
				cursor.execute(stmtAll, row["query_name"], row["query_type"], row["response_name"], row["response_type"], row["cname_name"], row["dname_name"], row["timestamp"]/1000, row["ip4_address"], row["ip6_address"], row["as_full"], row["country"], row["_cdn"])
			elif row["dname_count"]:
				dcount = row["dname_count"]
				ccount = row["cname_count"]
			else:
				for site, cnt in row:
					cursor.execute(stmtCDN, site, cnt)
	conn.close()
	return (dcount, ccount)

def main():
	#parse commandline arguments
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hay:")
	except getopt.GetoptError as err:
		logger.error(str(err))
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
		logger.error("Missing year!")
		sys.exit(1)
	
	#start database pool
	try:
		dbpool = DBCon.pooling.MySQLConnectionPool(pool_name="dbpool", pool_size=12)	
		dbpool.set_config(option_files='mysql_options.cnf')
	except DBCon.Error as err:
		logger.error("Cannot connect to MySQL!")
		logger.error(err)
		sys.exit(1)
	logger.info("DB Connection Pool started")

	#set arguments, start and run multiprocessing pool
	pool = Pool(processes=12, maxtasksperchild=1)
	for batch in batcher(glob.glob(os.path.join("/data", getpass.getuser(), "results", dataset, str(parseyear)+"-*", "*.json")), 12):
		#TODO Write bash script
		subprocess.run(['./decompression.sh'], check=True)
		arglist = []
		for file in batch:
			conn = dbpool.get_connection()
			arglist.append((parseyear, conn, file))
		results += pool.starmap(parse, arglist, 1)
	for dnames, cnames in results:
		dcount += dnames
		ccount += cnames
	logger.info("Domain names imported: %s", dcount)
	logger.info("CDN domains imported: %s", ccount)

if __name__ == "__main__":
	main()