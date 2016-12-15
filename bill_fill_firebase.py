from warnings import filterwarnings
#import MySQLdb as db
import sunlight
import os
from sunlight.pagination import PagingService
from firebase import firebase
import json
import requests
import time

'''
Example Data
 {
      "bill_resolution_type": "bill",
      "bill_type": "senate_bill",
      "bill_type_label": "S.",
      "committee_reports": [],
      "congress": 114,
      "current_status": "passed_bill",
      "current_status_date": "2016-12-13",
      "current_status_description": "This bill was passed by Congress on December 13, 2016 and goes to the President next.",
      "current_status_label": "Passed House & Senate",
      "display_number": "S. 8",
      "docs_house_gov_postdate": null,
      "id": 347162,
      "introduced_date": "2016-12-01",
      "is_alive": true,
      "is_current": true,
      "link": "https://www.govtrack.us/congress/bills/114/s8",
      "lock_title": false,
      "major_actions": [
        [
          "datetime.datetime(2016, 12, 1, 0, 0)",
          2,
          "Read twice and referred to the Committee on Foreign Relations.",
          "<action datetime=\"2016-12-01\" state=\"REFERRED\">\n      <text>Read twice and referred to the Committee on Foreign Relations.</text>\n    </action>\n    "
        ],
        [
          "datetime.datetime(2016, 12, 6, 0, 0)",
          3,
          "Committee on Foreign Relations. Ordered to be reported without amendment favorably.",
          "<calendar datetime=\"2016-12-06\" state=\"REPORTED\">\n      <text>Committee on Foreign Relations. Ordered to be reported without amendment favorably.</text>\n    </calendar>\n    "
        ],
        [
          "datetime.datetime(2016, 12, 10, 0, 0)",
          5,
          "Passed Senate without amendment by Unanimous Consent.",
          "<vote how=\"by Unanimous Consent\" type=\"vote\" datetime=\"2016-12-10\" where=\"s\" result=\"pass\" state=\"PASS_OVER:SENATE\">\n      <text>Passed Senate without amendment by Unanimous Consent.</text>\n      <reference ref=\"CR S7103-7104\" label=\"consideration\"/>\n      <reference ref=\"CR S7103-7104\" label=\"text as passed Senate\"/>\n    </vote>\n    "
        ],
        [
          "datetime.datetime(2016, 12, 13, 14, 39, 19)",
          9,
          "On passage Passed without objection.",
          "<vote how=\"without objection\" type=\"vote2\" datetime=\"2016-12-13T14:39:19-05:00\" where=\"h\" result=\"pass\" state=\"PASSED:BILL\">\n      <text>On passage Passed without objection.</text>\n      <reference ref=\"CR H7585\" label=\"text of measure as passed\"/>\n    </vote>\n    "
        ]
      ],
      "noun": "bill",
      "number": 8,
      "related_bills": [],
      "senate_floor_schedule_postdate": null,
      "sliplawnum": null,
      "sliplawpubpriv": null,
      "source": "thomas-congproj",
      "source_link": null,
      "sponsor": {
        "bioguideid": "C001071",
        "birthday": "1952-08-24",
        "cspanid": 1021114,
        "firstname": "Bob",
        "gender": "male",
        "gender_label": "Male",
        "id": 412248,
        "lastname": "Corker",
        "link": "https://www.govtrack.us/congress/members/bob_corker/412248",
        "middlename": "",
        "name": "Sen. Bob Corker [R-TN]",
        "namemod": "",
        "nickname": "",
        "osid": "N00027441",
        "pvsid": "65905",
        "sortname": "Corker, Bob (Sen.) [R-TN]",
        "twitterid": "SenBobCorker",
        "youtubeid": "senatorcorker"
      },
      "sponsor_role": {
        "caucus": null,
        "congress_numbers": [
          113,
          114,
          115
        ],
        "current": true,
        "description": "Junior Senator from Tennessee",
        "district": null,
        "enddate": "2019-01-03",
        "extra": {
          "address": "425 Dirksen Senate Office Building Washington DC 20510",
          "contact_form": "http://www.corker.senate.gov/public/index.cfm/emailme",
          "fax": "202-228-0566",
          "office": "425 Dirksen Senate Office Building",
          "rss_url": "http://www.corker.senate.gov/public/index.cfm/rss/feed"
        },
        "id": 42938,
        "leadership_title": null,
        "party": "Republican",
        "person": 412248,
        "phone": "202-224-3344",
        "role_type": "senator",
        "role_type_label": "Senator",
        "senator_class": "class1",
        "senator_class_label": "Class 1",
        "senator_rank": "junior",
        "senator_rank_label": "Junior",
        "startdate": "2013-01-03",
        "state": "TN",
        "title": "Sen.",
        "title_long": "Senator",
        "website": "http://www.corker.senate.gov"
      },
      "title": "S. 8: A bill to provide for the approval of the Agreement for Cooperation Between the Government of the United States of America and the Government of the Kingdom of Norway Concerning Peaceful Uses of Nuclear Energy.",
      "title_without_number": "A bill to provide for the approval of the Agreement for Cooperation Between the Government of the United States of America and the Government of the Kingdom of Norway Concerning Peaceful Uses of Nuclear Energy.",
      "titles": [
        [
          "official",
          "introduced",
          "A bill to provide for the approval of the Agreement for Cooperation Between the Government of the United States of America and the Government of the Kingdom of Norway Concerning Peaceful Uses of Nuclear Energy."
        ],
        [
          "display",
          null,
          "A bill to provide for the approval of the Agreement for Cooperation Between the Government of the United States of America and the Government of the Kingdom of Norway Concerning Peaceful Uses of Nuclear Energy."
        ]
      ]
    },
'''
firebase = firebase.FirebaseApplication('https://civis-site-generator.firebaseio.com', None)

params = {  "grant_type" : "client_credentials" }
r = requests.get('https://www.govtrack.us/api/v2/bill?order_by=-current_status_date&limit=600').json()
curr = list(r['objects'])

while True:
    try:
        time.sleep(5)
        for i, leg_i in enumerate(curr):
            # import ipdb; ipdb.set_trace()
            val = leg_i['bill_type']
            chamber = 'house'
            if val == 'senate_bill':
                chamber = 'senate'
            firebase.post('/bills', data={'billId':leg_i['id'], 'chamber':chamber, 'status':leg_i['current_status_description'], 'text':leg_i['display_number'], 'link':leg_i['link'], 'summary':leg_i['title'], 'createdAt':leg_i['introduced_date']})
            del curr[i]

    except e:
        print(e)
