"""This python utility, by the Sunlight Foundation, imports bulk data from the Center for Responsive Politics into a MySQL database, sparing the user the repetitious work of importing, naming fields and properly configuring tables. 

It includes a few auxillary tables and fields not part of CRP's official bulk download, but does not harness Personal Financial Disclosures. When you run this script repeatedly, it will check the updated dates and only re-download if the data has been modified. Even so, you are encouraged to download bulk files, which can be quite large, at non-peak-traffic times. Because some table schemas have changed over cycles, the utility has been tested only for recent cycles: 2008 and 2010. 

Register for a 'MyOpenSecrets' account at opensecrets.org and supply your login info below. Create a mysql database on your computer or a Web server and provide the host, user, password and database name as well. Also set the 'cycles' list for the two-digit representation of the election cycle you want to download. Then, run python sun-crp.py. 

Windows users can connect to this database in Microsoft Access if you prefer by setting up an ODBC connection. (Start-Control Panel-Administrative Tools-Data Sources (ODBC)). After you've set up an ODBC connection using the MySQL ODBC Connector, go to the External Data tab in Access, click 'other' and 'ODBC,' and connect to the tables. 

Luke Rosiak, March 2010 -- lrosiak@sunlightfoundation.com
"""


import os, sys, logging, MySQLdb, csv, urllib, re
import pyExcelerator
from download import CRPDownloader

CYCLES = ["10",]

CRP_EMAIL = ''  #Your MyOpenSecrets login
CRP_PASSWORD = ''
MYSQL_HOST = "localhost" 
MYSQL_USER = "root" #Your MySQL login
MYSQL_PASSWORD = ""
MYSQL_DB = "sun_crp" #create this database manually before running


def extract(src_path, dest_path):
    
    for f in os.listdir(src_path):
        
        fpath = os.path.join(src_path, f)
    
        if f.endswith('.zip'):
            cmd = 'unzip -u %s -d %s' % (fpath, dest_path)
        else:
            cmd = 'cp %s %s' % (fpath, dest_path)
            
        print cmd
        os.system(cmd)


def createtables():
    queries = ["DROP TABLE IF EXISTS cmtes;",
    """CREATE TABLE cmtes (
	Cycle char(4) NOT NULL,
	CmteID char(9) NOT NULL,
	PACShort varchar(40) NULL,
	Affiliate varchar(40) NULL,
	UltOrg varchar(40) NULL,
	RecipID char(9) NULL,
	RecipCode char(2) NULL,
	FECCandID char(9) NULL,
	Party char(1) NULL,
	PrimCode char(5) NULL,
	Src char(10) NULL,
    Sens char(1) NULL,
	Frgn int NOT NULL,
	Actve int NULL,
    PRIMARY KEY (Cycle, CmteID)
    );""",

    "DROP TABLE IF EXISTS cands;",
    """CREATE TABLE cands(
	Cycle char(4) NOT NULL,
	FECCandID char(9) NOT NULL,
	CID char(9) NOT NULL,
	FirstLastP varchar(40) NULL,
	Party char(1) NULL,
	DistIDRunFor char(4) NULL,
	DistIDCurr char(4) NULL,
	CurrCand char(1) NULL,
	CycleCand char(1) NULL,
	CRPICO char(1) NULL,
	RecipCode char(2) NULL,
	NoPacs char(1) NULL,
    PRIMARY KEY (Cycle, FECCandID),
    INDEX (CID)
    );""", 

    "DROP TABLE IF EXISTS indivs;",    
    """CREATE TABLE indivs(
	Cycle char(4) NOT NULL,
	FECTransID char(7) NOT NULL,
	ContribID char(12) NULL,
	Contrib varchar(34) NULL,
    RecipID char(9) NULL,
	Orgname varchar(40) NULL,
	UltOrg varchar(40) NULL,
	RealCode char(5) NULL,
	Date date NOT NULL,
	Amount int NULL,
    street varchar(20) NULL,
	City varchar (18) NULL,
	State char (2) NULL,
    Zip char (5) NULL,
	Recipcode char (2) NULL,
	Type char(3) NULL,
	CmteID char(9) NULL,
	OtherID char(9) NULL,
	Gender char(1) NULL,
	FECOccEmp varchar(35) NULL,
    Microfilm varchar(11) NULL,
	Occ_EF varchar(38) NULL,
	Emp_EF varchar(38) NULL,
    Src char(5) NULL,
    lastname varchar(20),
    first varchar(10),
    first3 varchar(3),
    INDEX (Orgname),
    PRIMARY KEY (Cycle, FECTransID)
    );""",

    "DROP TABLE IF EXISTS pacs;",
    """CREATE TABLE pacs (
	Cycle char(4) NOT NULL,
	FECRecNo char(7)  NOT NULL,
    PACID char(9)  NOT NULL,
	CID char(9)  NULL,
	Amount int,
	Date datetime NULL,
	RealCode char(5)  NULL,
	Type char(3)  NULL,
	DI char(1)  NOT NULL,
	FECCandID char(9)  NULL,
    INDEX (Cycle, PACID)
    );""",
   
    "DROP TABLE IF EXISTS pac_other;",
    """CREATE TABLE pac_other (
	Cycle char(4) NOT NULL,
	FECRecNo char(7)  NOT NULL,
	FilerID char(9)  NOT NULL,
	DonorCmte varchar(40)  NULL,
	ContribLendTrans varchar(40)  NULL,
	City varchar(18)  NULL,
	State char(2)  NULL,
	Zip char(5)  NULL,
	FECOccEmp varchar(35)  NULL,
	PrimCode char(5)  NULL,
	Date datetime NULL,
	Amount float NULL,
	RecipID char(9)  NULL,
	Party char(1)  NULL,
	OtherID char(9)  NULL,
	RecipCode char(2)  NULL,
	RecipPrimcode char(5)  NULL,
	Amend char(1)  NULL,
	Report char(3)  NULL,
	PG char(1)  NULL,
	Microfilm char(11)  NULL,
	Type char(3)  NULL,
	Realcode char(5)  NULL,
	Source char(5)  NULL
    );""",

    "DROP TABLE IF EXISTS expends;",
    """CREATE TABLE expends(
	Cycle char(4) NOT NULL,
    recordnum INT NULL,
	TransID char(20) ,
	CRPFilerid char(9) ,
	recipcode char(2) ,
	pacshort varchar(40) ,
	CRPRecipName varchar(90) ,
	ExpCode char(3) ,
	Amount decimal(12, 0) NOT NULL,
	Date datetime NULL,
	City varchar(18) ,
	State char(2) ,
	Zip char(9) ,
	CmteID_EF char(9) ,
	CandID char(9) ,
	Type char(3) ,
	Descrip varchar(100) ,
	PG char(5) ,
	ElecOther varchar(20) ,
	EntType char(3) ,
	Source char(5) 
    );""", 

    "DROP TABLE IF EXISTS lobbying;",
    """CREATE TABLE lobbying(
	uniqid varchar(56) NOT NULL,
	registrant_raw varchar(95) NULL,
	registrant varchar(40) NULL,
	isfirm char(1) NULL,
	client_raw varchar(95) NULL,
	client varchar(40) NULL,
	ultorg varchar(40) NULL,
	amount float NULL,
	catcode char(5) NULL,
	source char (5) NULL,
	self char(1) NULL,
	IncludeNSFS char(1) NULL,
	usethis char(1) NULL,
	ind char(1) NULL,
	year char(4) NULL,
	type char(4) NULL,
	typelong varchar(50) NULL,
	orgID char(10) NULL,
	affiliate char(1) NULL,
    PRIMARY KEY (uniqid)
    );""",

    "DROP TABLE IF EXISTS lobbyists;",
    """CREATE TABLE lobbyists(
	uniqID varchar(56) NOT NULL,
	lobbyist varchar(50) NULL,
	lobbyist_raw varchar(50) NULL,
	lobbyist_id char(15) NULL,
	year varchar(5) NULL,
	Offic_position varchar(100) NULL,
	cid char (12) NULL,
	formercongmem char(1) NULL,
    INDEX u (uniqID)
    );""",

    "DROP TABLE IF EXISTS lobbyindus;",
    """CREATE TABLE lobbyindus(
	client varchar(40) NULL,
	sub varchar(40) NULL,
	total float NULL,
	year char(4) NULL,
	catcode char(5) NULL
    );""",

    "DROP TABLE IF EXISTS lobbyagency;",
    """CREATE TABLE lobbyagency(
	uniqID varchar(56) NOT NULL,
	agencyID char(4) NOT NULL,
	Agency varchar(80) NULL,
    INDEX u (uniqID)
    );""",

    "DROP TABLE IF EXISTS lobbyissue;",
    """CREATE TABLE lobbyissue(
	SI_ID int NOT NULL,
	uniqID varchar(56) NOT NULL,
	issueID char(3) NOT NULL,
	issue varchar(50) NULL,
	SpecificIssue varchar(255) NULL,
	year char (4) NULL
    );""",

    "DROP TABLE IF EXISTS lob_bills;",
    """CREATE TABLE lob_bills(
	B_ID int NULL,
	si_id int NULL,
	CongNo char(3) NULL,
    Bill_Name varchar(15) NOT NULL
    );""",

    "DROP TABLE IF EXISTS lob_rpt;",
    """CREATE TABLE lob_rpt(
	TypeLong varchar (50) NOT NULL,
	Typecode char(4) NOT NULL
    );""",

   "DROP TABLE IF EXISTS categories;",
    """CREATE TABLE categories(
	catcode varchar (5) NOT NULL,
	catname varchar (50) NOT NULL,
	catorder varchar (3) NOT NULL,
	industry varchar (20) NOT NULL,
	sector varchar (20) NOT NULL,
	sectorlong varchar (200) NOT NULL,
    PRIMARY KEY (catcode)
    );""",

   "DROP TABLE IF EXISTS members;",
    """CREATE TABLE members(
	congno INT NOT NULL,
	cid varchar (9) NOT NULL,
	CRPName varchar (50) NOT NULL,
	party varchar (1) NOT NULL,
	office varchar (4) NOT NULL,
    PRIMARY KEY (congno, cid)
    );""",

   "DROP TABLE IF EXISTS congcmtes;",
    """CREATE TABLE congcmtes(
	code varchar(5) NOT NULL,
	title varchar (70) NOT NULL,
	INDEX (code)
    );""",

   "DROP TABLE IF EXISTS congcmte_posts;",
    """CREATE TABLE congcmte_posts(
	cid varchar(9) NOT NULL,
	congno INT NOT NULL,
	code varchar(5) NOT NULL,
	position varchar (20) NOT NULL
    );""", 

    "DROP TABLE IF EXISTS expendcodes;",
    """CREATE TABLE expendcodes(
	expcode varchar(3) NOT NULL,
	descrip_short varchar(20) NOT NULL,
	descrip varchar(50) NOT NULL,
	sector varchar(1) NOT NULL,
	sectorname varchar(50) NOT NULL,
    PRIMARY KEY (expcode)
    );""",

    "DROP TABLE IF EXISTS leadpacs;",
    """CREATE TABLE leadpacs(
	cid varchar(10) NOT NULL,
	cmteid varchar(10) NOT NULL
	);""",]


    expendcodes = """0	not yet coded	not yet coded	0	Uncoded
A00	Admin-Misc	Miscellaneous Administrative	A	Administrative
A10	Admin-Travel	Travel	A	Administrative
A20	Admin-Salaries	Salaries & Benefits	A	Administrative
A30	Admin-Postage	Postage/Shipping	A	Administrative
A50	Admin-Consultants	Administrative Consultants	A	Administrative
A60	Admin-Rent/Utilities	Rent/Utilities	A	Administrative
A70	Admin- Food/Meetings	Food/Meetings	A	Administrative
A80	Admin-Supplies/Equip	Supplies, Equipment & Furniture	A	Administrative
C00	Misc Campaign	Miscellaneous Campaign 	C	Campaign Expenses
C10	Campaign Materials	Materials	C	Campaign Expenses
C20	Campaign Polling	Polling/Surveys/Research	C	Campaign Expenses
C30	GOTV Campaign	GOTV	C	Campaign Expenses
C40	Campaign Events	Campaign Events	C	Campaign Expenses
C50	Campaign Consultants	Political Consultants	C	Campaign Expenses
C60	Campaign Direct Mail	Campaign Direct Mail	C	Campaign Expenses
F00	Misc Fundraising	Miscellaneous Fundraising	F	Fundraising
F40	Fundraising Events	Fundraising Events	F	Fundraising
F50	Fundraising Consult	Fundraising Consultants	F	Fundraising
F60	Direct Mail/TeleMkt	Fundr Direct Mail/Telemarketing	F	Fundraising
H00	Misc-Other	Miscellaneous	H	Other
H10	Misc-Donations	Charitable Donations	H	Other
H20	Misc-Loan Payments	Loan Payments	H	Other
M00	Misc Media	Miscellaneous Media	M	Media
M10	Broadcast Media	Broadcast Media	M	Media
M20	Print Media	Print Media	M	Media
M30	Internet Media	Internet Media	M	Media
M50	Media Consultants	Media Consultants	M	Media
N99	Non-Expenditure	Non-Expenditure	N	Non-Expenditure
R00	Misc Contribs	Miscellaneous Contributions	R	Contributions
R10	Party Contrib	Parties (Fed & Non-federal)	R	Contributions
R20	Candidate Contrib	Candidates (Fed & Non-federal)	R	Contributions
R30	Committee Contrib	Committees (Fed & Non-Federal)	R	Contributions
R90	Contrib Refunds	Contrib Refunds	R	Contributions
T00	Misc Transfer	Miscellaneous Transfer	T	Transfers
T10	Federal Transfer	Federal Transfer	T	Transfers
T20	Non-Federal Transfer	Non-Federal Transfer	T	Transfers
T30	Natl Party Transfer	National Party Transfer	T	Transfers
T60	St/Loc Pty Transfer	State/Local Party Transfer	T	Transfers
U10	Insufficient Info	Insufficient Info	U	Unknown
U20	Unknown	Unknown	U	Unknown"""
    recs = expendcodes.split("\n")
    for rec in recs:
        fields = rec.split("\t")
        query = "INSERT INTO expendcodes VALUES ('"+fields[0]+"', '"+fields[1]+"', '"+fields[2]+"', '"+fields[3]+"', '"+fields[4]+"');"
        queries.append(query)
 
    cursor = db.cursor()
    for query in queries: 
        #try:
        cursor.execute(query)
        #except:
        #    logging.info( "FAILED: " + query )

def writerowsfromcsv(file, table):
    def linereader(path):
        infile = open(path, 'rU')
        for line in infile:
            line = unicode(line, 'ascii', 'ignore')
            line = line.replace('\n', '')
            yield line
        infile.close()
    
    detailReader =  csv.reader(linereader(file), quotechar='|')
    writerows(detailReader, table)

def writerows(rows, table):

    def reformatdate(date):
        return date[6:] + '-' + date[:2] + '-' + date[3:5]


    cursor = db.cursor()
    for row in rows:
        if len(row)>0:
            if table=='indivs':
                lastname = row[3].split(', ')[0]
                first = row[3][len(lastname)+2:]
                row.append(lastname)
                row.append(first)
                row.append(first[:3])
                row[8] = reformatdate(row[8])
            if table=='pacs':
                row[5] = reformatdate(row[5])
            if table=='pac_other':
                row[10] = reformatdate(row[10])
            if table=='expends':
                row[9] = reformatdate(row[9])
            if table=='lobbyagency':
                row[1] = row[1][:3]


            sql = "INSERT INTO " + table + " VALUES ("
            for f in row:
                f = f.decode('iso8859-1').encode('utf-8','ignore').strip()
                sql = sql+' %s,'
            sql = sql[:-1]+");"
            try:
                cursor.execute(sql,row) 
            except:
                logging.info( "This FAILED:" + sql + str(row) )
                pass

    


def parseExcelIDs(file):
    def sheetToRows(values):
        matrix = [[]]
        for row_idx, col_idx in sorted(values.keys()):
            v = values[(row_idx, col_idx)]
            if isinstance(v, unicode):
                v = v.encode('cp866', 'backslashreplace')
            else:
                v = str(v)
            last_row, last_col = len(matrix), len(matrix[-1])
            while last_row < row_idx:
                matrix.extend([[]])
                last_row = len(matrix)

            while last_col < col_idx:
                matrix[-1].extend([''])
                last_col = len(matrix[-1])

            matrix[-1].extend([v])
        return matrix


    grabsheets = [('Members', 'members', [0,2,3,4,5]), ('CRP Industry Codes', 'categories', [0,1,2,3,4,5]), ('Congressional Cmte Codes', 'congcmtes',[0,1]), ('Congressional Cmte Assignments', 'congcmte_posts', [0,2,3,4])] 

    for sheet_name, values in pyExcelerator.parse_xls(file): 
        matrix = [[]]
        sheet_title = sheet_name.encode('cp866', 'backslashreplace')
        for sheet_info in grabsheets:
            if sheet_title.startswith(sheet_info[0]):
                matrix = sheetToRows(values)
                newmatrix = []
                for row in matrix:
                    if len(row)>0 and not row[1].startswith("This information is being made available"):
                        newrow = []
                        for i in sheet_info[2]:
                            try:                        
                                newrow.append(row[ i ])
                            except:
                                if sheet_info[1]=='congcmte_posts': 
                                    newrow = [row[0], row[2], row[3], '']
                                else:
                                    print str(row) + " failed"
                        newmatrix.append(newrow)
                writerows(newmatrix,sheet_info[1])



if __name__ == '__main__':

    src_path = 'download'
    dest_path = 'raw'
    if not os.path.exists(dest_path):
        os.mkdir(dest_path)
    if not os.path.exists(src_path):
        os.mkdir(src_path)

    logging.basicConfig(level=logging.DEBUG)
    
    if '--meta' in sys.argv:
        dl = CRPDownloader(CRP_EMAIL, CRP_PASSWORD, cycles=CYCLES)
        for res in dl.get_resources():
            print res    
    else:
        redownload = '--all' in sys.argv

    redownload = False
        
    dl = CRPDownloader(CRP_EMAIL, CRP_PASSWORD, path=src_path, cycles=CYCLES)
    for res in dl.get_resources():
        print res   
    dl.go(redownload=redownload)
    
    extract(src_path, dest_path)

    db = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD,db=MYSQL_DB)

    createtables()

    ext = ".txt"

    for year in CYCLES:
        writerowsfromcsv( os.path.join(dest_path, "cmtes" + year + ext), "cmtes")
        writerowsfromcsv( os.path.join(dest_path, "cands" + year + ext), "cands")
        writerowsfromcsv( os.path.join(dest_path, "indivs" + year + ext), "indivs")
        writerowsfromcsv( os.path.join(dest_path, "pacs" + year + ext), "pacs")
        writerowsfromcsv( os.path.join(dest_path, "pac_other" + year + ext), "pac_other")
        writerowsfromcsv( os.path.join(dest_path, "expends" + year + ext), "expends")

    writerowsfromcsv( os.path.join(dest_path, "lob_lobbying.txt"), "lobbying") 
    writerowsfromcsv( os.path.join(dest_path, "lob_lobbyist.txt"), "lobbyists") 
    writerowsfromcsv( os.path.join(dest_path, "lob_indus.txt"), "lobbyindus")
    writerowsfromcsv( os.path.join(dest_path, "lob_agency.txt"), "lobbyagency")
    writerowsfromcsv( os.path.join(dest_path, "lob_issue.txt"), "lobbyissue") 
    writerowsfromcsv( os.path.join(dest_path, "lob_bills.txt"), "lob_bills")
    writerowsfromcsv( os.path.join(dest_path, "lob_rpt.txt"), "lob_rpt")

    parseExcelIDs(os.path.join(dest_path,"CRP_IDs.xls"))
    
    f = urllib.urlopen("http://www.opensecrets.org/pacs/industry.php?txt=Q03&cycle=2010")
    l = f.read().replace('\n','').replace('\r','').replace('\t','')
    r = r'strID=C(\d*)">(.{5,50})</a></td><td><a href="/politicians/summary.php\?cid=N(\d{8})'
    ma = re.findall(r, l)
    rows = []
    for m in ma:
        rows.append(["N"+m[0], "C"+m[2]])
    writerows(rows,"leadpacs")      
