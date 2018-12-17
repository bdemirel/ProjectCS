
#    q1.py (-a) -p n -d YYYY-MM-DD

#imports
import re, json, logging
import glob, sys, getopt, os
from multiprocessing import Pool

sys.path.append(os.path.expanduser('~/.local/lib/python3.6/site-packages'))
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
def parse(avrofile):
    q_types = ['AAAA', 'A']
    r_types = ['AAAA', 'A', 'CNAME']
    fields = ['query_type', 'query_name', 'response_name', 'response_type', 'rtt', 'timestamp', 'worker_id', 'ttl', 'status_code', 'ip4_address', 'ip6_address', 'country', 'as_full', 'cname_name', 'dname_name']
    datalist = []
    cdns = {}
    dcounter = ccounter = 0
    logger.info("File started: %s", avrofile)
    f = open('results/'+avrofile[7:-4]+'json', 'w')
    reader = block_reader(open(avrofile, "rb"))
    for block in reader:
        for data in block:
        	sweden = True
        	if avrofile[7:12] == "alexa":
        		sweden = False
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
		            filtd.clear()
    datalist.insert(0, cdns)
    datalist.insert(0, {'dname_count':dcounter, 'cname_count':ccounter})
    json.dump(datalist, f, indent=2)
    f.close()

#main
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hap:d:")
    except getopt.GetoptError as err:
        logger.error(str(err))
        sys.exit(1)

    num_processor = 1
    folder = "source/open-tld/"
    for opt, val in opts:
        if opt == "-h":
        	print("q1.py (-a) -p <number_of_processes> -d <date_to_parse>")
        elif opt == "-a":
        	folder = "source/alexa/"
        elif opt == "-p":
            num_processor = int (val)
        elif opt == "-d":
        	date = val
    logger.info("Number of processes: %d", num_processor)
    pool = Pool(processes=num_processor, maxtasksperchild=1)
    pool.map(parse, glob.glob(folder+date+'/*.avro'), chunksize=1)
    logger.info("Execution successfully finished!")

if __name__ == "__main__":
	main()
