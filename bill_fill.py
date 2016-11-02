from warnings import filterwarnings
import MySQLdb as db
import sunlight
from sunlight.pagination import PagingService

'''
Example Data
 {
    "bill_id": "hr5930-114",
    "bill_type": "hr",
    "chamber": "house",
    "committee_ids": [
        "HSJU",
        "HSJU10"
    ],
    "congress": 114,
    "cosponsors_count": 40,
    "enacted_as": null,
    "history": {
        "active": false,
        "awaiting_signature": false,
        "enacted": false,
        "vetoed": false
    },
    "introduced_on": "2016-07-25",
    "last_action_at": "2016-08-10",
    "last_version": {
        "version_code": "ih",
        "issued_on": "2016-07-25",
        "version_name": "Introduced in House",
        "bill_version_id": "hr5930-114-ih",
        "urls": {
            "html": "https://www.gpo.gov/fdsys/pkg/BILLS-114hr5930ih/html/BILLS-114hr5930ih.htm",
            "pdf": "https://www.gpo.gov/fdsys/pkg/BILLS-114hr5930ih/pdf/BILLS-114hr5930ih.pdf",
            "xml": "https://www.gpo.gov/fdsys/pkg/BILLS-114hr5930ih/xml/BILLS-114hr5930ih.xml"
        },
        "pages": 12
    },
    "last_version_on": "2016-07-25",
    "last_vote_at": null,
    "number": 5930,
    "official_title": "To establish the Commission on the Social Status of Black Men and Boys, to study and make recommendations to address social problems affecting Black men and boys.",
    "popular_title": null,
    "related_bill_ids": [],
    "short_title": "Commission on the Social Status of Black Men and Boys Act",
    "sponsor": {
        "first_name": "Frederica",
        "last_name": "Wilson",
        "middle_name": "S.",
        "name_suffix": null,
        "nickname": null,
        "title": "Rep"
    },
'''

paging_service = PagingService(sunlight.congress)

leg = paging_service.bills(limit=600)

filterwarnings('ignore', category = db.Warning)
'''+---------+--------------+------+-----+---------+-------+
| Field   | Type         | Null | Key | Default | Extra |
+---------+--------------+------+-----+---------+-------+
| bill_id | int(11)      | NO   | PRI | 0       |       |
| summary | varchar(45)  | YES  |     | NULL    |       |
| name    | varchar(255) | YES  |     | NULL    |       |
| result  | varchar(255) | YES  |     | NULL    |       |
| chamber | varchar(255) | NO   | PRI |         |       |
+---------+--------------+------+-----+---------+-------+
'''
try:
	db_name = 'congress'
	con = db.connect(user='', passwd='')
	cur = con.cursor()

	# Create new database
	cur.execute('use congress;')

	for i, leg_i in enumerate(leg):
	    # cur.execute
	    cur.execute('INSERT INTO ' + db_name + '.bills '
	    '(bill_id, status, name, result, chamber, summary)'
	    'VALUES (' +str(leg_i['bill_id']) + ',"' + leg_i['status']+ '","' + leg_i['short_title']+'","' + leg_i['history']['enacted']+'","' + str(leg_i['chamber']) +'","' +leg_i['official_title']+'");',
	    )
	    print i

	cur.close()
	con.commit()
except Exception, e:
	print 'Error. Last query: ' + str(cur._last_executed)
	print e

print 'DB installation script finished'
