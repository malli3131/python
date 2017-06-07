#!/usr/bin/python

from ftplib import FTP
from filecmp import dircmp
import hashlib
import MySQLdb as mysql
import commands
import subprocess
import os

#Script helper functions........
def ftplogin(hostname, username, password):
    ftpcon = FTP(hostname)
    ftpcon.login(user=username, passwd=password)
    return ftpcon

def getFiles(hostname, username, password, dirPath):
    ftpcon = ftplogin(hostname, username, password)
    ftpcon.cwd(dirPath)
    files = ftpcon.nlst("*.txt")
    return files

def ftp_file_copy(ftp_hostname, ftp_username, ftp_password, ftp_filename, localPath, ftp_dirPath):
    ftpcon = ftplogin(ftp_hostname, ftp_username, ftp_password)
    file = localPath + "/" + ftp_filename
    print file
    with open(file, "wb") as f:
        ftpcon.cwd(ftp_dirPath)
        ftpcon.retrbinary("RETR " + ftp_filename, f.write)

def md5Checcksumftp(ftp_hostname, ftp_username, ftp_password, ftp_filename, ftp_dirpath):
    ftpcon = ftplogin(ftp_hostname, ftp_username, ftp_password)
    ftpcon.cwd(ftp_dirpath)
    m = hashlib.md5()
    ftpcon.retrbinary('RETR %s' % ftp_filename, m.update)
    return m.hexdigest()

def md5Checcksumlocal(filename, local_path):
    ffile = local_path + "/" + filename
    print "Local file {}".format(ffile)
    with open(ffile, 'rb') as getmd5:
        m = getmd5.read()
    	gethashlocal= hashlib.md5(m).hexdigest()
    	return gethashlocal


def compare_cksum(ftp_cksum, local_cksum):
    if(ftp_cksum == local_cksum):
    	return True
    else:
	return False

def isInHdfs(filename, hdfspath):
    hdfs_file = hdfspath + "/" + filename
    process = subprocess.Popen(["hadoop", "fs", "-ls", hdfs_file], stdout=subprocess.PIPE)
    result = process.stdout.readlines()
   
    return len(result)

def copyToHDFS(filename, local_path, hdfspath):
    local_file = local_path + "/" + filename
    process = subprocess.Popen(["hadoop", "fs", "-put", local_file, hdfspath], stdout=subprocess.PIPE)
    #process1= subprocess.Popen(["hadoop", "fs", "-checksum", local_file], stdout=subprocess.PIPE)
    #result1= process1.stdout.readlines()
    result = process.stdout.readlines()
    #print "checksum value is {}".format(result1)

def hdfs(filename, local_path, hdfspath, hostname, port, user, password, database, fileno):
    count = isInHdfs(filename, hdfspath)
    print "Count {}".format(count)
    if count < 1:
	copyToHDFS(filename, local_path, hdfspath)
	hdfslocal_cksum = md5Checcksumhdfs(filename,hdfspath)
        file_name=(os.path.splitext(filename)[0])
        print "file_name value is {}".format(file_name)
	status = createHiveTable(file_name)
	updateFileStatus(hostname, port, user, password, database, fileno, hdfslocal_cksum)
        return "Copied to HDFS"
    else:
	return "Already Exists in hdfs"

def getPendingFiles(hostname, port,user, password, database):
    pendingFiles = {}
    dbcon = mysql.connect(host=hostname, port=port, user=user, passwd=password, db=database)
    cursor = dbcon.cursor()
    query = "select * from cloverleaf_ingestion where status='pending' and pk=38;"
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
	pendingFiles[row[0]] = row[1]
    dbcon.commit()
    dbcon.close()
    return pendingFiles

def md5Checcksumhdfs(filename, hdfs_path):
    ffile = hdfs_path + "/" + filename
    print "Hdfs file {}".format(ffile)
    gethashhdfs = subprocess.Popen(["hadoop", "fs", "-checksum", ffile], stdout=subprocess.PIPE)
    return gethashhdfs.stdout.readline().split("\t")[2]

def updateFileStatus(hostname, port, user, password, database, fileno, hdfslocal_cksum):
    dbcon = mysql.connect(host=hostname, port=port, user=user, passwd=password, db=database)
    cursor = dbcon.cursor()
    status = "PROCESSED"
    hdfs_processing_status="PROCESSED"
    cursor.execute("""UPDATE cloverleaf_ingestion SET status=%s ,hdfs_processing_status=%s,hdfs_processing_datetime=now(),hdfs_checksum=%s WHERE pk=%s""", (status, hdfs_processing_status, hdfslocal_cksum, fileno ))
    dbcon.commit()
    dbcon.close()

def createHiveTable(tablename):
    create_stg="create external table if not exists testing." + tablename + "(emp_id int ,emp_name string) row format delimited fields terminated by '|' stored as TEXTFILE"
    print create_stg
    process = subprocess.Popen(["hive", "-e", create_stg], stdout=subprocess.PIPE)
    status = process.communicate()[0], process.returncode
    return status
    
    
#Script Logic starts Here..........
ftp_hostname = "172.29.127.81"
ftp_username = "root"
ftp_password = "Sentryntt"
ftp_dirpath = "/root/Desktop"
local_path = "/root/test"
hdfspath="/data/sentry_staging_data"
mysql_host = "172.29.127.80"
mysql_user = "cloverleaf"
mysql_port = 3307
mysql_password = "cloverleaf"
mysql_database = "cloverleafdb"

ftp_dirs = getPendingFiles(mysql_host, mysql_port, mysql_user, mysql_password, mysql_database)
for fileno in ftp_dirs:
    ftp_dir = ftp_dirs[fileno]
    ftp_files = getFiles(ftp_hostname, ftp_username, ftp_password, ftp_dir)
    for ftp_file in ftp_files:
    	print "ftp file: {}".format(ftp_file)
    	ftp_file_copy(ftp_hostname, ftp_username, ftp_password, ftp_file, local_path, ftp_dirpath)
    	ftp_cksum = md5Checcksumftp(ftp_hostname, ftp_username, ftp_password, ftp_file,ftp_dirpath)
    	local_cksum = md5Checcksumlocal(ftp_file,local_path)
        status =  compare_cksum(ftp_cksum, local_cksum)
        message = hdfs(ftp_file, local_path, hdfspath, mysql_host, mysql_port, mysql_user, mysql_password, mysql_database, fileno)
	print message
