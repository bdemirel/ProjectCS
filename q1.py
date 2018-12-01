"""
    q1.py -p n -d YYYY-MM-DD
"""

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import re, json, logging
import glob, sys, getopt
from multiprocessing import Pool

def parse(avrofile):	
    q_types = ['AAAA', 'A', 'MX']
    r_types = ['AAAA', 'A', 'MX', 'CNAME']
    fields = ['query_type', 'query_name', 'response_name', 'response_type', 'rtt', 'timestamp', 'worker_id', 'ttl', 'status_code', 'ip4_address', 'ip6_address', 'country', 'as', 'as_full', 'cname_name', 'dname_name', 'mx_address', 'mx_preference', 'ns_address', 'soa_mname', 'soa_rname', 'soa_expire']
    datalist = []
    dcounter = ccounter = 0;
    logging.info("File started: %s", avrofile)
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
                    logging.info("CDN Domain: %s", dname)
                    ccounter += 1
    datalist.insert(0, {'dname_count':dcounter, 'cname_count':ccounter})
    json.dump(datalist, f, indent=2)
    f.close()
    reader.close()

def main():
    logging.basicConfig(
        format='%(asctime)s %(process)d %(levelname)-8s %(message)s',
        handlers = [
            logging.FileHandler("q1.log"),
            logging.StreamHandler()],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:d:")
    except getopt.GetoptError as err:
        logging.error(str(err))
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
	logging.info("Number of processes: %d", num_processor)
    pool = Pool(processes=num_processor)
    pool.map(parse, glob.glob(folder+'/*.avro'))

if __name__ == "__main__":
    main()	
