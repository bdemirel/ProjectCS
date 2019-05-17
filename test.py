import sqlite3
from sqlite3 import Error

class test:
	def __init__(self):
		self.acnt = []
		self.ccnt = []

	def step(self, rname, rtype):
		if rtype == 'AAAA':
			self.acnt.append(rname)
		elif rtype == 'CNAME':
			self.ccnt.append(rname)

	def finalize(self):
		if len(self.ccnt) > 0 and len(self.acnt) > 1:
			for j in range(len(self.acnt)-1):
				if self.acnt[j] != self.acnt[j+1]:
					return 1

conn = sqlite3.connect("/home/bd/dragon/data/bdemirel/not-so-consistent.db")
conn.create_aggregate('test', 2, test)
sqlite3.enable_callback_tracebacks(True)
aa = conn.execute("SELECT SUM(i) FROM (SELECT test(response_name, response_type) AS i FROM `2018` WHERE query_type = 'AAAA' GROUP BY query_name, parsedate HAVING COUNT(*) > 1)").fetchall()
print(aa)