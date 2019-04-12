import json, collections, os, glob, logging, sys, getopt, sqlite3, getpass
import matplotlib.pyplot as plt
from sqlite3 import Error

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

try:
	conn = sqlite3.connect(os.path.join("/data", getpass.getuser(), "bdemirel.db"))
except Error as err:
	logger.error("Cannot connect to database!")
	logger.error(err)
	sys.exit(1)
logger.info("Database Connection started")

#create the data
dates = ["{}.{:02d}.{}".format(x, y, parseyear) for y in range(1,13) for x in ["01", "15"]]
cursor = conn.cursor()
stmtDom = "SELECT domain FROM cdn{} ORDER BY count DESC LIMIT 10".format(str(parseyear))
stmtFetch = "SELECT COUNT(rowid) FROM `{}` WHERE cname = ? AND parsedate = ?".format(str(parseyear))

cursor.execute(stmtDom)
cursor.arraysize = 10
domlist = cursor.fetchmany()
for domain in domlist:
    datalist = []
    for pdate in dates:
        cursor.execute(stmtFetch, (domain, pdate))
        datalist += cursor.fetchone()
    plt.plot(dates, datalist, label=domain)
plt.legend()
plt.xlabel("Time")
plt.ylabel("Occurance")
plt.title("10 Most Common CDN Providers throughout {}".format(str(parseyear)))
plt.savefig("mostcommon.png")

"""
#plot
plt.plot(dates, data)
plt.xticks(range(len(top_20)), list(top_20.keys()), rotation='vertical')
plt.subplots_adjust(bottom=0.4)
plt.title("CNAME domain results")"""