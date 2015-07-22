import pickle, os, time, sys, psycopg2, fnmatch
from datetime import datetime
from glob import glob
from operator import itemgetter

def folder_list():
    with open(r"C:\\apps\pyupsql\tiffolderlist.txt", "rb") as f:
        eps_folders_list = pickle.load(f)
        
    return eps_folders_list

path = "B:\DTP DEPARTMENT\LIBRARY TIF\*\*\*\*\*"
extension = "*.tif"

def timeStamped(pname, fname, fmt="{pname}/%Y-%m-%d_{fname}"):
    return datetime.now().strftime(fmt).format(fname=fname, pname=pname)

te = open(timeStamped("C:\\apps\pyupsql\logs", "application.log"), "w")

class Unbuffered:
    def __init__(self, stream):
        self.stream = stream
        
    def write(self, data):
        self.stream.write(data)
        self.stream.flush()
        te.write(data)

def get_last_up_date():
    f = open("C:\\apps\pyupsql\last_mod_date.txt", "r")
    last_mod_date = int(f.read())
    f.close()
    return last_mod_date

def set_last_up_date():
    f = open("C:\\apps\pyupsql\last_mod_date.txt", "w")
    f.write('%s' %(int(time.time())))
    print "Update time: %s" %(int(time.time()))
    f.close()

def hr_mod_date():
    the_mod_date = time.gmtime(get_last_up_date())
    the_mod_time = time.strftime("%m/%d/%Y %H:%M:%S", the_mod_date)
    return (the_mod_date, the_mod_time)

def list_modified_folders(list):
    dirinfo = []
    for name in list:
#        print mod_date(name)
        print "The folder %s was modified on %s" % (name, mod_date(name)[1])
        if mod_date(name)[0] > get_last_up_date():
            print "NEW"
            dirinfo.append(name)
            print language(name)
        else:
            print "OLD"
    return dirinfo
 
def language(name):
    return name.split('\\')[3]

def mod_date(x):
    d = os.stat(x).st_mtime
    f = datetime.fromtimestamp(d).strftime('%Y-%m-%d %H:%M:%S')
    return (d,f)

def insert_file_into_db(table, ext, filename, root):
    conn = establishConnection('filelist')
    cur = conn.cursor()
    cur.execute(""" INSERT INTO %s (FileType, FileName, FilePath) VALUES( '%s', '%s', '%s' ); 
                            """ %(table, ext, filename, root))
    conn.commit()

def search_file(table, filename):
    conn = establishConnection('filelist')
    cur = conn.cursor()
    cur.execute("""SELECT exists (SELECT 1 FROM %s WHERE filename = '%s' LIMIT 1);""" %(table, filename))
    exists = cur.fetchone()[0]
    return exists

def establishConnection(db):
    conn = psycopg2.connect("dbname='%s' user='postgres' host='localhost' password='123456'" %db)
    return conn

def update_db():
    i = 0 # this will count newly introduced files in database
    for dir in list_modified_folders(folder_list()):
        source = dir
        table = language(dir)
        for root, dirnames, filenames in os.walk(source):
            print("Processing %s" % root)
            for filename in fnmatch.filter(filenames, "%s" %extension):
                print filename
                print root
                ext = os.path.splitext(filename)[1][1:]
                print ext
                print search_file(table, filename)
                if (not search_file(table, filename)):
                    print "\nInserting file in database..."
                    print (table, ext, filename, root)
                    insert_file_into_db(table, ext, filename, root)
                    i += 1
                    print "\n"
    print "=" * 150
    print "New files inserted: %s" %i # print the number of files in the log

def pyupsql():
    print "Last update was done on: %s" %hr_mod_date()[1]
    print "Checking Libraries... Please wait..."
    update_db()
    set_last_up_date()
    print "\nTASK COMPLETED"
    te.close()
    time.sleep(3)

sys.stdout=Unbuffered(sys.stdout)
pyupsql()