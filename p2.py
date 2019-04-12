import sqlite3 as DBCon, logging, sys, glob, getpass, json, getopt, subprocess, os
from sqlite3 import Error

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

#start database connection
try:
	conn = DBCon.connect(os.path.join("/data", getpass.getuser(), "bdemirel.db"))
except Error as err:
	logger.error("Cannot connect to database!")
	logger.error(err)
	sys.exit(1)
logger.info("Database Connection started")

#parse commandline arguments
try:
	opts, args = getopt.getopt(sys.argv[1:], "hoy:")
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
	logger.info("Missing year!")
	sys.exit(1)

#set variables and start processing
results = []
dcount = ccount = 0
procdate = None
while True:
	#check for files
	batch = glob.glob(os.path.join("/data", getpass.getuser(), "results", dataset, "parse", "*.json"))
	if batch == []:
		if glob.glob(os.path.join("/data", getpass.getuser(), "results", dataset, str(parseyear)+"*.tar.gz")) != []:
			proc = subprocess.run(['./decompression.sh', str(parseyear), dataset], check=True, universal_newlines=True, stdout=subprocess.PIPE)
			procdate = proc.stdout[:8]
			batch = glob.glob(os.path.join("/data", getpass.getuser(), "results", dataset, "parse", "*.json"))
		else:
			break
	
	#prepare db statements
	cursor = conn.cursor()
	stmtCDN = "INSERT INTO cdn"+str(parseyear)+" (`domain`, `count`) VALUES (?, ?)"
	stmtCheck = "SELECT rowid FROM cdn"+str(parseyear)+" WHERE `domain` = ?"
	stmtUpdate = "UPDATE cdn"+str(parseyear)+" SET `count` = ? WHERE `rowid` = ?"
	stmtAll = "INSERT INTO `"+str(parseyear)+"` (query_name, query_type, response_name, response_type, cname, dname, parsedate, timestamp, ipv4, ipv6, as_full, country, cdn) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	#process the files
	for jsonfile in batch:
		logger.info("File started: %s", jsonfile)
		with open(jsonfile) as jf:
			contents = json.load(jf)
			for row in contents:
				if "_id" in row:
					cursor.execute(stmtAll, (row["query_name"], row["query_type"], row["response_name"], row["response_type"], row["cname_name"], row["dname_name"], procdate, row["timestamp"]/1000, row["ip4_address"], row["ip6_address"], row["as_full"], row["country"], row["_cdn"]))
				elif "dname_count" in row:
					dcount += row["dname_count"]
					ccount += row["cname_count"]
				else:
					for site, cnt in row.items():
						cursor.execute(stmtCheck, (site,))
						rowid = cursor.fetchone()
						if  rowid == None:
							cursor.execute(stmtCDN, (site, cnt))
						else:
							cursor.execute(stmtUpdate, (cnt, rowid[0]))
		jf.close()
		os.remove(jsonfile)
		conn.commit()
	cursor.close()

#close connection and print results
conn.close()
logger.info("Domain names imported: %s", dcount)
logger.info("CDN domains imported: %s", ccount)