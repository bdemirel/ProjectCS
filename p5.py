"""
    python3 p5.py -y YYYY -v A(AAA) -p n
    @param YYYY	= year in 4 digits, e.g: 2018
	@param A(AAA) = DNS query type for IP version, e.g: A for IPv4 and AAAA for IPv6
	@param n	= number of processes, e.g: 10
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

dates = ["{}{:02d}{}".format(parseyear, y, x) for y in range(1,2) for x in ["01", "15"]]
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

def query(domain):
	try:
		fconn = sqlite3.connect(dbloc)
	except Error as err:
		logger.error("Cannot connect to database!")
		logger.error(err)
		sys.exit(1)
	logger.info("Database Connection started")
	fconn.create_function("cdnDomain", 2, cdn)
	#conn.row_factory = sqlite3.Row
	#conn.set_trace_callback(callback)
	stmt = "SELECT cdnDomain(cname, cdn) FROM `{}` WHERE query_name = ? AND query_type = ? AND parsedate IN ({}, {}) ORDER BY parsedate ASC".format(parseyear, dates[0], dates[1])
	#logger.debug("Execute {}".format(domain))
	results = fconn.execute(stmt, (domain, ipv)).fetchall()
	results = [rows[0] for rows in results]
	#logger.debug(len(results))
	#results.insert(0, date)
	return results

def main():
	try:
		conn = sqlite3.connect(dbloc)
	except Error as err:
		logger.error("Cannot connect to database!")
		logger.error(err)
		sys.exit(1)

	stmtList = "SELECT query_name FROM `{}` WHERE parsedate = '20180101' AND query_type = ?".format(parseyear)
	queryList = conn.execute(stmtList, (ipv,)).fetchall()
	queryList = [query[0] for query in queryList]

	pool = Pool(concurrency)
	res = pool.map(query, queryList)
	#print(res)
	nodes = []
	links = []
	order = []
	domset = set()
	for j in range(len(res)):
		domset.add(res[j][0])
	for i in range(len(dates)):
		orderList = []
		for dom in domset:
			nodes.append({'id': dom+str(i), 'title': ''})
			orderList.append(dom+str(i))
		logger.debug("ORDAH!")
		logger.debug(len(res))
		order.append(orderList[:])
		if i != 0:
			for j in range(len(res)):
				linkObj = {'source': res[j][i-1]+str(i-1), 'target': res[j][i]+str(i), 'value': 1}
				linkKey = next((linkKey for linkKey, item in enumerate(links) if item['source'] == linkObj['source'] and item['target'] == linkObj['target']), None)
				if  linkKey == None:
					links.append(linkObj)
				else:
					links[linkKey]['value'] += 1
	resultsObj = {
		"nodes" : nodes,
		"links" : links,
		"order" : order,
		"alignTypes" : True
	}
	with open('results.json', 'w') as file:
		#file.write("{} ------ [p5.py] ------\n".format(time.asctime()))
		file.write(json.dumps(resultsObj))
		#file.write("\n\n")

if __name__ == "__main__":
	main()