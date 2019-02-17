import MySQLdb, re, sys, os
from glob import glob


# Define a folder (repository) that will hold the names of each property.
# backups_folder = "/home/alexander/Desktop/2018-07-17/mysql" # Ubuntu
backups_folder = sys.argv[1]
# properties_folder = "C:\\Users\\awuerz\\Desktop\\kato-csv"
# properties_folder = "/home/user/dev/kato-csv" # centos 



def restore_database(server, username, password, database): 

    full_path = backups_folder+"/"+database

	# Open database connection.
    connection = MySQLdb.connect(server, username, password)

	# Prepare the cursor object. 
    cursor = connection.cursor()

    with open(full_path, "r") as fl: 
        try: 
            cursor.execute('SET foreign_key_checks = 0')
            for sql in fl: 
                if sql == '\n' or sql[0] == '/':
                    pass
                else: 
                    sql = " ".join(fl.readlines())
                    # Execute the SQL command
                    cursor.execute(str(sql))

            # Commit changes to the database. 
            cursor.execute('SET foreign_key_checks = 1')
            connection.commit()
        except Exception as e: 
            print(e)
            # Rollback in case there was an error. 
            connection.rollback()
        # Close the connection to the database. 
        connection.close()

def exec_sql_file(server, username, password, sql_file):
	# Open database connection.
    connection = MySQLdb.connect(server, username, password)

	# Prepare the cursor object. 
    cursor = connection.cursor()
    print("\n[INFO] Executing SQL script file: '%s'" % (sql_file))
    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r'[^-;]+;', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            #print "\n\n[DEBUG] Executing SQL statement:\n%s" % (statement)
            try:
                cursor.execute(statement)
            except (OperationalError, ProgrammingError) as e:
                print "\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args))

            statement = ""
    connection.close()



# Define server, username, password
server = "localhost"
username = "root"
password = "password"



# every_backup_path = glob(backups_folder+"/*/") # linux
# for item in every_backup_path:

backups_folder_list = os.listdir(backups_folder)
    #every_backup_folder.append(item.split("/", )[-2]) # linux
    

print("Databases restored: \n")
for database in backups_folder_list:
    if (database != "mysql"): 
        # restore_database(server, username, password, database)
        exec_sql_file(server, username, password, backups_folder+"/"+database)
	    # print(str(backup))
