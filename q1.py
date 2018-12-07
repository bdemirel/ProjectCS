"""
    q1.py -p n -d YYYY-MM-DD
"""

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import re, json, logging
import glob, sys, getopt
from multiprocessing import Pool

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

def parse(avrofile):
    q_types = ['AAAA', 'A', 'MX']
    r_types = ['AAAA', 'A', 'MX', 'CNAME']
    fields = ['query_type', 'query_name', 'response_name', 'response_type', 'rtt', 'timestamp', 'worker_id', 'ttl', 'status_code', 'ip4_address', 'ip6_address', 'country', 'as', 'as_full', 'cname_name', 'dname_name', 'mx_address', 'mx_preference', 'ns_address', 'soa_mname', 'soa_rname', 'soa_expire']
    datalist = []
    dcounter = ccounter = 0
    logger.info("File started: %s", avrofile)
    f = open('results/'+avrofile[11:45]+'.json', 'w')
    reader = DataFileReader(open(avrofile, "rb"), DatumReader())
    for data in reader:
        if (data['query_type'] in q_types) and (data['response_type'] in r_types):
            dcounter += 1
            filtd = { newkey: data.get(newkey) for newkey in fields }
            datalist.append(filtd)
            domains = [data['query_name'], data['response_name']]
            for dname in domains:
                if dname == None:		dname = "None"
                if (re.search(r'(cdn)|(akamai)|(edge)|(cloudfront)|(cachefly)|(swift)|(fastly)|(llnwd)', dname, re.I)):
                    logger.debug("CDN Domain: %s", dname)
                    ccounter += 1
    datalist.insert(0, {'dname_count':dcounter, 'cname_count':ccounter})
    json.dump(datalist, f, indent=2)
    f.close()
    reader.close()

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:d:")
    except getopt.GetoptError as err:
        logger.error(str(err))
        sys.exit(1)

    num_processor = 1
    for opt, val in opts:
        if opt == "-h":
        	print "q1.py -p <number_of_processes> -d <date_to_parse>"
        elif opt == "-p":
            num_processor = int (val)
        elif opt == "-d":
        	folder = val
        else:
            assert False, "unhandled opt"
    logger.info("Number of processes: %d", num_processor)
    pool = Pool(processes=num_processor, maxtasksperchild=1)
    pool.map(parse, glob.glob(folder+'/*.avro'), chunksize=1)

if __name__ == "__main__":
	main()
