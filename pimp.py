"""
Reads a CSV file, replaces dates of the form: "aaa, dd aaa dddd dd:dd" with 'aaa dd aaa dddd dd:dd'
in order to prevent the comma forcing data into wrong columns when imported into SQL database.
"""

import re
import os
import sys
import time
import smtplib
import pysftp
import pyodbc
import logging

from paramiko import SSHException

logfile = '%s\\logs\\%s.log' % (os.getcwd(), time.asctime( time.localtime(time.time())).replace(" ", "-").replace(":","."))
logging.basicConfig(filename=logfile, filemode='w', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
error_msgs = {'FILE_IO1':"Error opening %s. Check the file name and/or access.", 'FILE_IO2':"Error opening %s or %s. Check the file name and/or access.", 
'NO_FTP':"Failed to open FTP connection.", 'FTP_E1':"FTP Error: Unable to access path %s.", 'ERR_FOLDER_L1':"Unable to create folder %s.",
'FTP_GET_R2':"FTP Error: Unable to get %s\%s.",'DB_CONN_2':"Unable to connect to %s:%s."
}
info_msgs = {'FILE_SCAN_L1':"Checking %s for unprocessed files.", 'PATH_FILE_L2':"Found local file: %s%s", 'FILE_SCAN_R1':"Checking FTP server %s for new files.",
'NO_WORK':"There were no files to be processed.", 'NO_FOLDER_L1':"Local folder %s does not exist, creating it.", 'DB_CONN_2':"Connected to %s:%s.",
'PROC_FILE2':"Processing %s. Destination table: %s", 'NEW_TABLE1':"Created table: %s"
}
settings = {}
user_fmt = "\n%s\n"
ERRORS, WARNINGS, COMPLETED = 0,0,0

def main():
	"""
	STAGE 1: parse & validate cmd line parameters
	"""
	args = len(sys.argv)	
	global settings, logfile
	global ERRORS, WARNINGS, COMPLETED

	append = False
	skipftp=False
	cwd = os.getcwd()
	rwd = ""
	
	if args <= 2:
		print "Usage: python pimp.py settings.txt paths.txt\n"
		exit()
		
	settingsfile = sys.argv[args-2]
	paths = []
	pathsfile = sys.argv[args-1]
	filequeue = []

	try:
		set = open(settingsfile) # open config files
		lookin = open(pathsfile)
	except IOError:
		print error_msgs['FILE_IO2'] % (settingsfile, lookin)
		exit()
		
	line = set.readline()
	while line != "":
		kv = line.split(":")
		settings[kv[0].strip()] = kv[1].strip()
		line = set.readline()
	
	# read list of paths we will look for files to import
	line = lookin.readline() # throw away header
	line = lookin.readline()
	while line != "":
		paths.append(line.strip())
		line = lookin.readline()
		
	set.close()
	lookin.close()
	
	"""
	STAGE 2: Aquire necessary files from designated sources
	always process local files first, in case script was interrupted before they could be processed
	"""

	for path in paths:
		if "folder:" in path: # local file system paths
			path = path.replace("folder:", cwd).strip()	
			print info_msgs['FILE_SCAN_L1'] % path
			try:
				os.chdir(path)
				files = os.listdir(path)
				for file in files:
					if os.path.isfile(file) and settings['filetypefilter'] in file:
						print info_msgs['PATH_FILE_L2'] % (path, file)
						filequeue.append("%s%s" % (path, file))				
			except OSError:
				# folder doesn't exist, which is fine, it may be created later
				pass

	os.chdir(cwd)
	
	# copy remote files locally 
	#skipftp = True #  debug only
	
	if settings['input'] == 'ftp' and skipftp == False: # init connection to ftp server
		print info_msgs['FILE_SCAN_R1'] % settings['ftpserver']
		try:
			ftpconnection = pysftp.Connection(host=settings['ftpserver'], username=settings['ftpuser'], password=settings['ftppwd'])
		except pysftp.ConnectionException:
			print user_fmt % error_msgs['NO_FTP']
			logging.warning(error_msgs['NO_FTP'])
			if len(filequeue) == 0: # no files, no ftp: no work
				logging.info(info_msgs['NO_WORK'])
				exit()
			else:
				skipftp = True # skip file scan but continue so local files can be processed
					
		if skipftp == False:
			# Get the directory and file listing
			repstr = "\\daily" # we don't care about report interval, only that file exists
			for path in paths:
				if "ftp:" in path:
					path = path.replace("ftp:", "").strip()
					try:
						ftpconnection.chdir(path)
					except IOError:
						print user_fmt % error_msgs['FTP_E1'] % path
						logging.debug(error_msgs['FTP_E1'] % path)
						ERRORS = ERRORS + 1

					rwd = ftpconnection.getcwd()
					localdest = "%s%s" % (cwd, rwd.replace("/", "\\"))
					localdest = localdest.replace(repstr, "")
					data = ftpconnection.listdir()
					rpfmt = "%s/%s"
					lpfmt = "%s\%s"
					for d in data:
						if ftpconnection.isfile(d):
							try:
								os.chdir(localdest)
							except OSError:
								print user_fmt % info_msgs['NO_FOLDER_L1'] % localdest
								try:
									os.mkdir(localdest)
								except OSError:
									print user_fmt % error_msgs['ERR_FOLDER_L1'] % localdest
									logging.debug(error_msgs['ERR_FOLDER_L1'] % localdest)
									ERRORS = ERRORS + 1

							if os.path.isfile("%s\%s" % (localdest, d)) == True:
								print "Destination file already exists. Skipping."
							else:		
								print "Copying %s/%s to %s\%s" % (rwd, d, localdest, d)
								try: # since the csv files have no unique row id, we need to move them so they aren't reprocessed
									ftpconnection.get(rpfmt % (rwd, d) , lpfmt % (localdest, d)) 
									filequeue.append(lpfmt % (localdest, d))
									ftpconnection.rename(rpfmt % (rwd, d), rpfmt % (rwd.replace("daily", "done").replace("zac_hourly", "done"), d)) # move remote file to done folder
								except IOError:
									print user_fmt % error_msgs['FTP_GET_R2'] % (rwd, d)
									logging.debug(error_msgs['FTP_GET_R2'] % (rwd, d))
									ERRORS = ERRORS + 1
		# Closes the FTP connection
		ftpconnection.close()
		
	"""
	STAGE 3: open connection to destination database
	"""
	if len(filequeue):
		#connection_str =  "Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;" % (settings['sqlserver'], settings['sqluser'], settings['sqlpwd'], settings['sqldb'])
		connection_str = "Driver={SQL Server Native Client 11.0};Provider=SQLNCLI11;Server=%s;Database=%s;Uid=%s;Pwd=%s;" % (settings['sqlserver'], settings['sqldb'], settings['sqluser'], settings['sqlpwd'])
		try:
			db_connection = pyodbc.connect(connection_str)
			db_connection.autocommit = True
			print(user_fmt % info_msgs['DB_CONN_2'] % (settings['sqlserver'], settings['sqldb']))
			logging.info(user_fmt % info_msgs['DB_CONN_2'] % (settings['sqlserver'], settings['sqldb']))
		except pyodbc.DatabaseError:
			print(user_fmt % error_msgs['DB_CONN_2'] % (settings['sqlserver'], settings['sqldb']))
			logging.info(user_fmt % error_msgs['DB_CONN_2'] % (settings['sqlserver'], settings['sqldb']))
			ERRORS = ERRORS + 1
		
		"""
		STAGE 4: process data files
		"""
		for filename in filequeue:
			load_csv(filename, db_connection)
	
		db_connection.close()
	
	print "Done!"
	print "Script successfully processed %d files.\nThere were %d errors and %d warnings encountered while processing %d files." % (COMPLETED, ERRORS, WARNINGS, len(filequeue))
	print "Check %s for details." % logfile
	
def load_csv(filename, db_connection):
	"""
	Copies a csv file (minus the top row) 
	& changes text dates/times to SQL standard compliant dates/times
	"""
	global settings, logfile, user_fmt, info_msgs, error_msgs
	global ERRORS, WARNINGS, COMPLETED
	try:
		infile = open(filename) # open in/out files
	except IOError:
		print(user_fmt % error_msgs['FILE_IO1'] % (filename))
		logging.debug(error_msgs['FILE_IO1'] % (filename))
		ERRORS = ERRORS + 1
		return
	outfilename = filename.replace(settings['filetypefilter'], ".imp")
	try:
		outfile = open(outfilename,"wb", 1)
	except IOError:
		print(user_fmt % error_msgs['FILE_IO1'] % (outfilename))
		logging.debug(user_fmt % error_msgs['FILE_IO1'] % outfilename)
		ERRORS = ERRORS + 1
		return
	pieces = filename.split('\\')#re.split('\W+', filename)
	imp_tbl_name = pieces[len(pieces)-1].strip()

	# strip out digits, dashes & other riffraff
	imp_tbl_name = re.sub('[0-9\-]*.csv$', '', imp_tbl_name).lower()
	print user_fmt % info_msgs['PROC_FILE2'] % (filename, imp_tbl_name)
	logging.info(info_msgs['PROC_FILE2'] % (filename, imp_tbl_name))
			
	lines = []
	line_num = 0
	net_diff = 0
	total_reps = 0
	bytes_read = 0
	bytes_written = 0
	total_matches = 0
	
	# regex for finding double quoted, comma separated dates eg. "Tue, 13 Oct 2015 17:35:21"
	msdate = r'("{1,1})([A-Z]{3,3})([ ,]{2,2})(([A-Z0-9 ]{12,12})([0-9:]+[0-9:]+)")'
	qcstr = r'(["]{1,1})[^"]*(["]{1,1})'
	tspan = r'([0-9]+):([0-9]+):([0-9]+)'
	rex1 = re.compile(msdate, re.IGNORECASE) # make sure to IGNORECASE or this won't work
	rex2 = re.compile(qcstr, re.IGNORECASE)
	rex3 = re.compile(tspan, re.IGNORECASE)
	
	header = infile.readline() # header row is not included in row count
	columns = header.split(settings['fielddelimiter'])
	#columns = list(set(columns)) # eliminate dupes
	#columns.append("UID") # add PK column name 

	col_names = ["[%s] varchar(62) NULL" % c.strip().replace(" ", "_") for c in columns]	
	col_names = ",\n".join(col_names) # reformat for create table statement 
	col_names = col_names.replace("_(Interval)", "2")
	col_ids = {}
	# build dictionary of column names for data length checking
	for n, e in enumerate(columns):
		col_ids[n] = "[%s]".replace(" ", "_") % e
	# redef important columns , replace specific fields of interest with preferred data type
	#col_names = col_names.replace("[UID] nvarchar(62) NULL", "[UID] int IDENTITY(1,1) PRIMARY KEY")
	col_names = col_names.replace("[DATE] varchar(62)", "DATE date")
	col_names = col_names.replace("[TIME] varchar(62)", "TIME time")
	col_names = col_names.replace("TIMESTAMP] varchar(62)", "TIMESTAMP] datetime2")
	# table/column specific overrides
	col_names = col_names.replace("[Campaign]", "[Campaign1]", 1) # Calllog has dupes	
	col_names = col_names.replace("[DNIS]", "[DNIS1]", 1) # Calllog has dupes
	col_names = col_names.replace("[LOGIN_TIMESTAMP] datetime2", "[LOGIN_TIMESTAMP] varchar(62)")	# agent table sometimes has 0 value, which won't load
	col_names = col_names.replace("street] varchar(62)", "street] varchar(MAX)") #street often has wacky chars 	
	# these redefs allow capturing large fields
	col_names = col_names.replace("[RECORDINGS] varchar(62)", "[RECORDINGS] varchar(MAX)")
	col_names = col_names.replace("[IVR_PATH] varchar(62)", "[IVR_PATH] varchar(MAX)")
	col_names = col_names.replace("[SKILL_AVAILABILITY] varchar(62)", "[SKILL_AVAILABILITY] varchar(MAX)")
	col_names = col_names.replace("[FACILITY_NAME] varchar(62)", "[FACILITY_NAME] varchar(MAX)")
	col_names = col_names.replace("[NOTES] varchar(62)", "[NOTES] varchar(MAX)")
	
	# main loop for reading a line of text at a time
	line = infile.readline()
	while line != "":
		line_num = line_num + 1
		bytes_read = bytes_read + len(line)
		line = "%s\r\n" % line.strip() 
		lines.append(line)
	
		#handle quoted, comma delimited timestamp fields
		for match in rex1.finditer(line): 
			total_matches = total_matches + 1
			total_reps = total_reps + 1
			line = line.replace(match.group(0), datetime2(match.group(0)))

		#handle more general case : quoted, comma delimited
		for match in rex2.finditer(line): 
			total_matches = total_matches + 1
			total_reps = total_reps + 1
			line = line.replace(match.group(0), clean(match.group(0)))
		
		field_check(line_num, line, col_ids)
		
		outfile.write(line)
		bytes_written = bytes_written + len(line)
		line = infile.readline()
		
	infile.close() # done with input file
	outfile.close() # also close the output file so we can import it in next step
	# report results so far
	logging.info("Read %d data lines (%d bytes) from input file." % (len(lines), bytes_read))
	logging.info("Found %d matches of pattern:%s" % (total_matches, msdate))
	logging.info("Made %d replacements.Wrote %d bytes to %s." % (total_reps, bytes_written, outfilename))
	
	if table_exists(db_connection, imp_tbl_name) == False:
		if create_table(db_connection, imp_tbl_name, col_names) == True:
			print user_fmt % info_msgs['NEW_TABLE1'] % imp_tbl_name
			logging.info(user_fmt % info_msgs['NEW_TABLE1'] % imp_tbl_name)
		else:
			ERRORS = ERRORS + 1
			return
	
	if bulk_insert(db_connection, imp_tbl_name, outfilename) == False:
		ERRORS = ERRORS + 1
	else:
		COMPLETED = COMPLETED + 1
		logging.info("PASS: BULK INSERTED %d rows into %s from %s.\n" % (len(lines), imp_tbl_name, outfilename))
		os.remove(filename)
		os.remove(outfilename)
	#truncate_table(db_connection, imp_tbl_name)	# for testing	
	
def table_def_spec():
	pass
	
def table_exists(db_connection, table_name):
	table_exists_sttmnt = "IF OBJECT_ID('%s') IS NOT NULL BEGIN SELECT 1 END" % table_name
	try:
		db_cursor = db_connection.cursor()
		db_cursor.execute(table_exists_sttmnt)
		db_connection.commit()
		rows = db_cursor.fetchall()
		if rows is not None:
			return True
		else:
			return False
	except pyodbc.ProgrammingError:
		pass
	return False
	
def create_table(db_connection, table_name, column_defs):
	create_table_sttmnt = "CREATE TABLE %s\n(\n%s\n)" % (table_name, column_defs)
	try:
		#print create_table_sttmnt
		db_cursor = db_connection.cursor()
		db_cursor.execute(create_table_sttmnt)
		db_connection.commit()
		return True
	except pyodbc.ProgrammingError:
		logging.debug("FAIL: %s" % create_table_sttmnt)
		pass
	return False

def truncate_table(db_connection, table_name):
	truncate_table_sttmnt = "TRUNCATE TABLE %s" % (table_name)
	try:
		db_cursor = db_connection.cursor()
		db_cursor.execute(truncate_table_sttmnt)
		db_connection.commit()
		return True
	except pyodbc.ProgrammingError:
		pass
	return False

def row_count(db_connection, table_name):
	try:
		db_cursor = db_connection.cursor()
		db_cursor.execute("SELECT COUNT(*) FROM %s" % table_name)
		db_connection.commit()
		rows = db_cursor.fetchall()
		if rows is not None:
			return rows
	except pyodbc.ProgrammingError:
		pass
	return None
	
def bulk_insert(db_connection, table_name, filename):
	global settings

	bulk_insert_sttmnt = "BULK INSERT %s FROM '%s' WITH (FIELDTERMINATOR = '%s')" % (table_name, filename, settings['fielddelimiter']) # KEEPIDENTITY, CHECK_CONSTRAINTS, 
	try:	
		db_cursor = db_connection.cursor()
		db_cursor.execute(bulk_insert_sttmnt)
		db_connection.commit()
		#print "\nSuccessfully executed BULK INSERT %s\n" % bulk_insert_sttmnt
		return True
	except pyodbc.DatabaseError:
		print("FAIL: %s" % bulk_insert_sttmnt)
		logging.debug("FAIL: %s" % bulk_insert_sttmnt)
		#exit()
	return False
	
def datetime2(str):
	dt2 = str.replace("\"", "")
	dt2 = dt2.split(" ")
	month = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dec":12}
	yyyy = dt2[3]
	mm = "%d" % month[dt2[2]]
	mm = mm.zfill(2)
	dd = "%s" % dt2[1]
	dd = dd.zfill(2)
	dt2 = "%s-%s-%s %s" % (yyyy, mm, dd, dt2[4]) # yyyy-mm-dd
	#print dt2
	return dt2
	
def field_check(r, str, col_ids):
	global settings, WARNINGS
	data = str.split(settings['fielddelimiter'])
	exempt_cols = [24, 37, 38, 43, 90, 91, 93, 94, 95, 115]
	for c, col in enumerate(data):
		if len(data[c]) > 62 and not (c in exempt_cols):
			logging.warning("Data truncation at row %d, col %d (%s)" % (r, c, col_ids.values()[c]))
			WARNINGS = WARNINGS + 1
			data[c] = data[c][0:62]
	
def clean(str):
	return str.replace(settings['fielddelimiter'], "").replace("\"", "")
		
if __name__ == "__main__":
    main()		
