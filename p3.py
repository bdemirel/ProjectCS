"""
    python3 p3.py -y YYYY -p n
    @param YYYY   = year in 4 digits, e.g: 2018
    @param n      = number of processes (also the number of top domains), most commonly used as 10
"""
import json, collections, os, glob, logging, sys, getopt, sqlite3, getpass, matplotlib.pyplot as plt
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

def callback(stmt):
    logger.info(stmt)

def query(domain):
    try:
        conn = sqlite3.connect(os.path.join("/data", getpass.getuser(), "consistent.db"))
    except Error as err:
        logger.error("Cannot connect to database!")
        logger.error(err)
        sys.exit(1)
    cursor = conn.cursor()
    #conn.set_trace_callback(callback)
    stmtFetch = "SELECT COUNT(*) FROM `{}` WHERE cname LIKE ? COLLATE NOCASE AND parsedate = ?".format(str(parseyear))

    dates = ["{}{:02d}{}".format(parseyear, y, x) for y in range(1,13) for x in ["01", "15"]]
    datalist = []
    for pdate in dates:
        cursor.execute(stmtFetch, ("%{}.".format(domain[0]), pdate))
        datalist += cursor.fetchone()
    return datalist

def main():
    try:
        conn = sqlite3.connect(os.path.join("/data", getpass.getuser(), "consistent.db"))
    except Error as err:
        logger.error("Cannot connect to database!")
        logger.error(err)
        sys.exit(1)
    logger.info("Database Connection started")

    #create the data
    cursor = conn.cursor()
    stmtDom = "SELECT domain FROM cdn{} ORDER BY count DESC LIMIT ?".format(str(parseyear))

    cursor.execute(stmtDom, [concurrency])
    cursor.arraysize = concurrency
    domlist = cursor.fetchmany()

    pool = Pool(processes=concurrency, maxtasksperchild=1)
    results = pool.map(query, domlist, chunksize=1)
    
    #prints results:
    #print("\n".join([','.join([str(item) for item in row]) for row in results]))

    dates = ["{}.{:02d}".format(x, y) for y in range(1,13) for x in ["01", "15"]]
    for i in range(len(results)):
        plt.plot(dates, results[i], label=domlist[i][0])

    plt.legend(loc='upper center', bbox_to_anchor=(1.45, 0.8))
    plt.xticks(rotation=90)
    plt.xlabel("Time")
    plt.ylabel("CNAME Redirections")
    plt.title("{} Most Popular CDN Providers in {}".format(concurrency, str(parseyear)))
    #plt.set_size_inches(20, 25)
    plt.subplots_adjust(bottom=0.2, right=0.6)
    plt.savefig("mostcommon.png", dpi=400)

if __name__ == "__main__":
    main()