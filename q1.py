
#    q1.py (-a) -p n -d YYYY-MM-DD

#imports
import re, json, logging, subprocess, glob, sys, getopt, os, datetime, getpass
from multiprocessing import Pool
from fastavro import block_reader
import tldextract

#logging
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = logging.FileHandler('q1.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)-8s %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)

#file parsing
def parse(dataset, parsedate, avrofile):
    q_types = ['AAAA', 'A']
    r_types = ['AAAA', 'A', 'CNAME']
    fields = ['query_type', 'query_name', 'response_name', 'response_type', 'rtt', 'timestamp', 'worker_id', 'status_code', 'ip4_address', 'ip6_address', 'country', 'as_full', 'cname_name', 'dname_name', 'response_ttl', 'soa_serial']
    datalist = []
    cdns = {}
    dcounter = ccounter = 0
    logger.info("File started: %s", avrofile)
    f = open(os.path.join('results', dataset, parsedate, os.path.basename(avrofile)), 'w')
    reader = block_reader(open(avrofile, "rb"))
    for block in reader:
        for data in block:
        	sweden = True if dataset == "alexa1m" else False
        	d_ext = tldextract.extract(data['query_name'])
        	if (sweden == True and d_ext[2] == "se") or (sweden == False):
		        if (data['query_type'] in q_types) and (data['response_type'] in r_types):
		            dcounter += 1
		            filtd = { newkey: data.get(newkey) for newkey in fields }
		            filtd['_id'] = dcounter
		            filtd['cdn'] = 0
		            if data['response_type'] == "CNAME":   
		                c_ext = tldextract.extract(data['cname_name'])
		                if d_ext[1] != c_ext[1]:
		                    ccounter += 1
		                    filtd['cdn'] = 1
		                    cdomain = c_ext[1]+"."+c_ext[2]
		                    logger.debug("CDN Domain Found: %s", data['cname_name'])
		                    if cdomain not in cdns:
		                        cdns[cdomain] = 0
		                    cdns[cdomain] += 1
		            datalist.append(filtd)
    datalist.insert(0, cdns)
    datalist.insert(0, {'dname_count':dcounter, 'cname_count':ccounter})
    json.dump(datalist, f, indent=2)
    f.close()

#main
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hap:d:r:")
    except getopt.GetoptError as err:
        logger.error(str(err))
        sys.exit(1)

    num_processor = 1
    dataset = "open-tld"
    repeat = 1
    for opt, val in opts:
        if opt == "-h":
            print("q1.py (-a) -p <number_of_processes> -d <date_to_parse> (-r <number_of_repetitions>)")
            sys.exit(0)
        elif opt == "-a":
        	dataset = "alexa1m"
        elif opt == "-p":
            num_processor = int (val)
        elif opt == "-d":
        	parsedate = datetime.date(int (val[0:4]), int (val[5:7]), int (val[8:10]))
        elif opt == "-r":
            repeat = int (val)
    
    for iteration in range(repeat):
        subprocess.run(['./routine.sh', parsedate.strftime("%Y"), parsedate.strftime("%d %b"), dataset], check=True)
        logger.info("Number of processes: %d", num_processor)
        pool = Pool(processes=num_processor, maxtasksperchild=1)
        argset = []
        for file in glob.glob(os.path.join("/data", getpass.getuser(), dataset, parsedate.strftime("%Y%m%d"), "*.avro")):
            argset.append((dataset, parsedate.strftime("%Y%m%d"), file))
        pool.starmap(parse, argset, chunksize=1)
        logger.info("Day "+str(iteration+1)+" successfully finished!")
        if parsedate.day == 1:
            parsedate = parsedate.replace(day=15)
        elif parsedate.day == 15:
            parsedate = parsedate.replace(day=1)
            parsedate = parsedate.replace(month=parsedate.month+1)
    logger.info("Execution successfully finished!")

if __name__ == "__main__":
	main()
