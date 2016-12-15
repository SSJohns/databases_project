from warnings import filterwarnings
#import MySQLdb as db
import sunlight
import os
from sunlight.pagination import PagingService
from firebase import firebase
import json

'''
Example Data
{u'last_name': u'Davidson', u'state_name': u'Ohio', u'office': u'1011 Longworth House Office Building', u'thomas_id': u'02296', u'first_name': u'Warren', u'middle_name': None, u'district': 8, u'title': u'Rep', u'in_office': True, u'state': u'OH', u'term_end': u'2017-01-03', u'crp_id': u'N00038767', u'oc_email': None, u'party': u'R', u'fec_ids': [u'H6OH08315'], u'votesmart_id': 166760, u'website': None, u'fax': None, u'leadership_role': None, u'govtrack_id': u'412675', u'phone': u'202-225-6205', u'birthday': u'1970-03-01', u'term_start': u'2016-06-09', u'nickname': None, u'contact_form': None, u'ocd_id': u'ocd-division/country:us/state:oh/cd:8', u'bioguide_id': u'D000626', u'gender': u'M', u'name_suffix': None, u'chamber': u'house'}
'''
paging_service = PagingService(sunlight.congress)
firebase = firebase.FirebaseApplication('https://civis-site-generator.firebaseio.com', None)

leg = paging_service.legislators(limit=1000)

output = list()
for i, leg_i in enumerate(leg):
	if leg_i['in_office'] != True:
		continue
	ti = leg_i['thomas_id']
	st = leg_i['state']
	ln = leg_i['last_name']
	fn = leg_i['first_name']
	if 'district' not in leg_i:
		dt = 'None'
	else:
		dt = leg_i['district']
	ch = leg_i['chamber']
	output.append({'email':leg_i["oc_email"], 'userId':ti, 'first_name':fn, 'last_name':ln, 'chamber':ch, 'state':st })
	firebase.post('/congress_states/'+st, data={'email':leg_i["oc_email"], 'thomas_id':ti, 'first_name':fn, 'last_name':ln, 'chamber':ch, 'state':st, 'district':dt,'term_end':leg_i['term_end'], 'party':leg_i['party'],'govtrack_id': leg_i['govtrack_id'],'ocd_id':leg_i['ocd_id'], 'bioguide_id':leg_i['bioguide_id'] })
