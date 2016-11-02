from warnings import filterwarnings
import MySQLdb as db
import sunlight
from sunlight.pagination import PagingService

'''
Example Data
{u'last_name': u'Davidson', u'state_name': u'Ohio', u'office': u'1011 Longworth House Office Building', u'thomas_id': u'02296', u'first_name': u'Warren', u'middle_name': None, u'district': 8, u'title': u'Rep', u'in_office': True, u'state': u'OH', u'term_end': u'2017-01-03', u'crp_id': u'N00038767', u'oc_email': None, u'party': u'R', u'fec_ids': [u'H6OH08315'], u'votesmart_id': 166760, u'website': None, u'fax': None, u'leadership_role': None, u'govtrack_id': u'412675', u'phone': u'202-225-6205', u'birthday': u'1970-03-01', u'term_start': u'2016-06-09', u'nickname': None, u'contact_form': None, u'ocd_id': u'ocd-division/country:us/state:oh/cd:8', u'bioguide_id': u'D000626', u'gender': u'M', u'name_suffix': None, u'chamber': u'house'}
'''

paging_service = PagingService(sunlight.congress)

leg = paging_service.legislators(limit=600)

filterwarnings('ignore', category = db.Warning)
try:
	db_name = 'congress'
	con = db.connect(user='', passwd='')
	cur = con.cursor()

	# Create new database
	cur.execute('use congress;')

	# Create PARAMETERS table
	# '''
	# cur.execute('DROP TABLE IF EXISTS ' + db_name + '.PARAMETERS;')
	# query = ('CREATE TABLE ' + db_name + '.PARAMETERS ('
	# 'idPARAMETERS INT(10) NOT NULL AUTO_INCREMENT, '
	# 'Param_name VARCHAR(30) NULL DEFAULT NULL, '
	# 'Param_value VARCHAR(255) NULL DEFAULT NULL, '
	# 'Timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP '
	# 'ON UPDATE CURRENT_TIMESTAMP, '
	# 'User_id VARCHAR(20) NULL DEFAULT NULL, '
	# 'PRIMARY KEY (idPARAMETERS) );'
	# )
	# cur.execute(query)
	# '''
	for i, leg_i in enumerate(leg):
	    # cur.execute
	    # pp.pprint(leg_i)
	    if 'district' not in leg_i:
		cur.execute('INSERT INTO ' + db_name + '.congress_people '
		'(congress_id, state, last_name, first_name, district, chamber) '
		'VALUES (' +str(leg_i['thomas_id']) + ',"' + leg_i['state']+ '","' + leg_i['last_name']+'","' + leg_i['first_name']+'","' + 'None' +'","' +leg_i['chamber']+'");',
		)
		continue
	    cur.execute('INSERT INTO ' + db_name + '.congress_people '
	    '(congress_id, state, last_name, first_name, district, chamber) '
	    'VALUES (' +str(leg_i['thomas_id']) + ',"' + leg_i['state']+ '","' + leg_i['last_name']+'","' + leg_i['first_name']+'","' + str(leg_i['district']) +'","' +leg_i['chamber']+'");',
	    )
	    print i

	cur.close()
	con.commit()
except Exception, e:
	print 'Error. Last query: ' + str(cur._last_executed)
	print e

print 'DB installation script finished'
