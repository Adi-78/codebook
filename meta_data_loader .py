import sys   
import os
import string
import mysql.connector as mariadb
from mysql.connector import Error
import re
import shutil # rmtree Method Belongs to This Module

try:
#MariaDB connection block

	db = mariadb.connect(user='<mysql_user>',password='<mysql_password>',host='<mysql_host>',database='<mysql_database>')
	cursor = db.cursor()

#retrieving information

	cursor.execute("SELECT id,client_id,dns_id,monikar_name,db_name,user_name,dir_path,linux_usr,linux_pwd,coredb_name FROM <mysql_table> WHERE process_flag=0")

	records = cursor.fetchall()

	
	print ("\nFetching Below Details from '<mysql_table>' table.\n")

	for row in records:
			print("id 		= ","'"+str(row[0])+"'")
			print("client_id 	= ","'"+str(row[1])+"'")
			client_id = row[1]
			print("dns_id  	= ","'"+str(row[2])+"'")
			dsn_id = row[2]
			print("monikar_name 	= ","'"+row[3] +"'")
			client_name = row[3]
			print("db_name  	= ","'"+ row[4]+"'")
			db_name = row[4]
			print("user_name  	= ","'"+ row[5]+"'")
			user_name = row[5]
			print("dir_path  	= ","'"+ row[6]+"'")
			dir_path = row[6]
			print("linux_usr  	= ","'"+ row[7]+"'")
			linux_usr = row[7]
			print("linux_pwd  	= ","'"+ row[8]+"'")
			linux_pwd = row[8]
			print("coredb_name	= ","'"+ row[9]+"'", "\n")
			server_name = row[9]
   

# Checking User Inputs is Empty or Not.

	if (db_name != "" and client_name != "" and server_name != "" and client_id != "" and dsn_id != "" and user_name != "" and dir_path != ""):
	
		#Creating Directory

		path = os.path.join(dir_path, db_name)
		
		if os.path.exists(path):
			shutil.rmtree(path)
			print("\nWarning: Already Exists Directory Removed.")
			#sys.exit() 
		os.makedirs(path)
		print ("\nSuccess: Directory '%s' Created." % path)
		
		args = [db_name,client_name,server_name,client_id,dsn_id,user_name,path] #Passing Parameters to Procedure
		#cursor = db.cursor()	
		cursor.callproc('sp_metadata', args)
		db.commit()
		cursor.close()
			
			

		meta_file 	= [f for f in os.listdir(path) if re.search("(new_customer_metadata_"+ user_name +"|pmi_auth_client_db_config_data_"+ user_name+ ")", f) and f.endswith(".txt")]
		
		if meta_file != "":
		
					
			# Copying Metadata Files To NAS

			print("\nMeta Files are Ready to Transfer to NAS Please Verify the Details Below.")
			print("\nFiles: %s" % meta_file)
			print("\nTarget Server: %s" % server_name)
			print("\nTarget Directory Path: %s" % os.path.join('<Target Directory Path To Linux Server>', user_name))
		else:
			print("Error: Meta Files Could Not be Created.")
			sys.exit()

	else:
		print("\nError: All Parameters are Required, Kindly Try Again.")
		sys.exit()


	
	var_run = input("\nPlease Enter 'Y' To Transfer The Files or 'N' to Exit: ")
	
	if (var_run in ("Y,y")):
		print("")
		command = 'pscp -pw ' + linux_pwd + ' ' + path + '/*.txt ' + linux_usr + '@' + server_name +'.<DNS>:<Target Directory Path of Linux Server>' + user_name + '/'
	
		os.system(command)	
	
		#print(command)
		#sys.exit()
	
	elif (var_run in ("N,n")):

		print("\nBye...")
	
		sys.exit()	

except Error as e :
    print ("\nError While Connecting to MySQL", e)
finally:
    #closing database connection.
    if(db.is_connected()):
        db.close()
        	


	






	
