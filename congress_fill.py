from warnings import filterwarnings
import MySQLdb as db
import sunlight
import os
from sunlight.pagination import PagingService

'''
Example Data
{u'last_name': u'Davidson', u'state_name': u'Ohio', u'office': u'1011 Longworth House Office Building', u'thomas_id': u'02296', u'first_name': u'Warren', u'middle_name': None, u'district': 8, u'title': u'Rep', u'in_office': True, u'state': u'OH', u'term_end': u'2017-01-03', u'crp_id': u'N00038767', u'oc_email': None, u'party': u'R', u'fec_ids': [u'H6OH08315'], u'votesmart_id': 166760, u'website': None, u'fax': None, u'leadership_role': None, u'govtrack_id': u'412675', u'phone': u'202-225-6205', u'birthday': u'1970-03-01', u'term_start': u'2016-06-09', u'nickname': None, u'contact_form': None, u'ocd_id': u'ocd-division/country:us/state:oh/cd:8', u'bioguide_id': u'D000626', u'gender': u'M', u'name_suffix': None, u'chamber': u'house'}
'''
USER_DB = os.environ['USER_DB']
PASS_DB = os.environ['PASS_DB']
paging_service = PagingService(sunlight.congress)

leg = paging_service.legislators(limit=600)

filterwarnings('ignore', category = db.Warning)
try:
	db_name = 'congress'
	con = db.connect(user=USER_DB, passwd=PASS_DB)
	cur = con.cursor()

	# Create new database
	# cur.execute('use congress;')

	for i, leg_i in enumerate(leg):
        	ti = oursql.IterWrapper(leg_i['thomas_id'])
        	st = oursql.IterWrapper(leg_i['state'])
        	ln = oursql.IterWrapper(leg_i['last_name'])
        	fn = oursql.IterWrapper(leg_i['first_name'])
		if 'district' not in leg_i:
	    		dt = oursql.IterWrapper('None')
		else:
			dt = oursql.IterWrapper(leg_i['district'])
        	ch = oursql.IterWrapper(leg_i['chamber'])
	    	cur.execute('INSERT INTO {}.congress_people (congress_id, state, last_name, first_name, district, chamber) VALUES (?,?,?,?,?,?);'.format(db_name),
	    	(ti,st,ln,fn,dt,ch) )
	    	print i

	cur.close()
	con.commit()
except Exception, e:
#	print 'Error. Last query: ' + str(cur._last_executed)
	print e

print 'DB installation script finished'
