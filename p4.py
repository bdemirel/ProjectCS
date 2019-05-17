"""
    python3 p4.py -y YYYY
    @param YYYY   = year in 4 digits, e.g: 2018
"""
import os, logging, sys, getopt, sqlite3, getpass
import matplotlib.pyplot as plt
import matplotlib.transforms as tforms
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
dbloc = os.path.join("/data", getpass.getuser(), "consistent.db")

def callback(stmt):
    logger.debug(stmt)

def query(stmt):
    try:
        conn = sqlite3.connect(dbloc)
    except Error as err:
        logger.error("Cannot connect to database!")
        logger.error(err)
        sys.exit(1)
    cursor = conn.cursor()
    logger.info("Database Connection started")
    #conn.set_trace_callback(callback)

    dates = ["{}{:02d}{}".format(parseyear, y, x) for y in range(1,13) for x in ["01", "15"]]
    datalist = []
    for i in range(23):
        logger.debug("Loop {}".format(i))
        cursor.execute(stmt, (dates[i], dates[i+1]))
        datalist += cursor.fetchone()
    return datalist

def main():
    stmts = [
    "SELECT COUNT(*) FROM `{0}` a INNER JOIN `{0}` b ON a.query_name = b.query_name AND a.query_type = b.query_type WHERE a.parsedate = ? AND b.parsedate = ? AND a.cdn = 0 AND b.cdn = 1".format(str(parseyear)),
    "SELECT COUNT(*) FROM `{0}` a INNER JOIN `{0}` b ON a.query_name = b.query_name AND a.query_type = b.query_type WHERE a.parsedate = ? AND b.parsedate = ? AND a.cdn = 1 AND b.cdn = 0".format(str(parseyear)),
    #"SELECT COUNT(*) FROM `{0}` a INNER JOIN `{0}` b ON a.query_name = b.query_name AND a.query_type = b.query_type WHERE a.parsedate = ? AND b.parsedate = ? AND a.cdn = 0 AND b.cdn = 0 AND a.cname = b.cname".format(str(parseyear)),
    "SELECT COUNT(*) FROM `{0}` a INNER JOIN `{0}` b ON a.query_name = b.query_name AND a.query_type = b.query_type WHERE a.parsedate = ? AND b.parsedate = ? AND a.cdn = 1 AND b.cdn = 1 AND a.cname = b.cname".format(str(parseyear)),
    "SELECT COUNT(*) FROM `{0}` a INNER JOIN `{0}` b ON a.query_name = b.query_name AND a.query_type = b.query_type WHERE a.parsedate = ? AND b.parsedate = ? AND a.cdn = 1 AND b.cdn = 1 AND a.cname != b.cname".format(str(parseyear))
    ]
    labels = [
        "New CDN users", 
        "Leaving CDN users", 
        #"non-CDN users with no change",
        "CDN users with no change", 
        "CDN users who swapped providers"
    ]
    
    pool = Pool(processes=len(stmts), maxtasksperchild=1)
    results = pool.map(query, stmts, chunksize=1)

    #prints results:
    #print("\n".join([','.join([str(item) for item in row]) for row in results]))

    fig = plt.figure()
    splt = fig.add_subplot(111)
    #offset = tforms.Affine2D().translate(0.5, 0)

    dates = ["{}.{:02d}".format(x, y) for y in range(1,13) for x in ["01", "15"]]
    dates.pop(0)
    for i in range(len(results)):
        splt.plot(dates, results[i], label=labels[i])

    plt.legend(loc='center', bbox_to_anchor=(0.7, 0.75))
    plt.xticks(rotation=90)
    plt.xlabel("Time")
    plt.ylabel("CNAME Redirections")
    plt.title("Basic Trends Among Websites about CDN Providers in {}".format(str(parseyear)))
    #plt.set_size_inches(20, 25)
    plt.subplots_adjust(bottom=0.2)
    plt.savefig("btrends.png", dpi=400)

if __name__ == "__main__":
    main()
