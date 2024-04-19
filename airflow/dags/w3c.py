import datetime as dt
import csv
import airflow
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json
import mysql.connector
import logging
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
import logging
import csv


BaseDir="/opt/airflow/data"
RawFiles=BaseDir+"/Raw/"
Staging=BaseDir+"/Staging/"
StarSchema=BaseDir+"/StarSchema/"
# DimIP = open(Staging+'DimIP.txt', 'r')
# DimUnicIP=open(Staging+'DimIPUniq.txt', 'w')
# uniqCommand="sort "+Staging+"DimIP.txt | uniq > "+Staging+'DimIPUniq.txt'

uniqCommand="sort -u "+Staging+"DimIP.txt > "+Staging+'DimIPUniq.txt'
uniqDateCommand="sort -u "+Staging+"DimDate.txt > "+Staging+'DimDateUniq.txt'

# uniqCommand="sort -u -o "+Staging+"DimIPUniq.txt " +Staging+"DimIP.txt"
# 2>"+Staging+"errors.txt"
try:   
   os.mkdir(BaseDir)
except:
   print("Can't make BaseDir")
try:
   os.mkdir(RawFiles)
except:
   print("Can't make BaseDir") 
try: 
   os.mkdir(Staging)
except:
   print("Can't make BaseDir") 
try:
   os.mkdir(StarSchema)
except:
   print("Can't make BaseDir") 


def insert_data_into_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # File and table configuration
    data_config = [
        {
            "file_path": '/opt/airflow/data/StarSchema/CleanOutFact1.txt',
            "table_name": 'fact_table',
            "columns": "(Date, Time, Browser, IP, ResponseTime, RequestedFile)"
        },
        {
            "file_path": '/opt/airflow/data/StarSchema/DimDateTable.txt',
            "table_name": 'dim_date',
            "columns": "(Date, Year, Month, Day, DayofWeek)"
        },
        {
            "file_path": '/opt/airflow/data/StarSchema/DimIPLoc.txt',
            "table_name": 'dim_iploc',
            "columns": "(IP, Country_Code, Country_Name, City, Latitude, Longitude)"
        }
    ]

    # Process each data configuration
    for config in data_config:
        insert_data_from_file(cursor, config['file_path'], config['table_name'], config['columns'])

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data inserted successfully into all tables.")

def insert_data_from_file(cursor, file_path, table_name, columns):
    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header
            placeholders = ', '.join(['%s'] * len(columns.split(',')))
            insert_query = f'INSERT INTO {table_name} {columns} VALUES ({placeholders})'
            for row in reader:
                try:
                    cursor.execute(insert_query, row)
                except Exception as e:
                    logging.error(f"Error inserting row {row}: {e}")
                    raise e
    except Exception as e:
        logging.error(f"General error during data import in {table_name}: {e}")
        raise e

    




def validate_and_clean_data():
    input_file_path = '/opt/airflow/data/StarSchema/OutFact1.txt'
    clean_file_path = '/opt/airflow/data/StarSchema/CleanOutFact1.txt'
    invalid_data_path = '/opt/airflow/data/StarSchema/InvalidOutFact1.txt'

    with open(input_file_path, 'r') as infile, \
         open(clean_file_path, 'w') as outfile, \
         open(invalid_data_path, 'w') as errfile:

        headers = infile.readline()
        outfile.write(headers)
        errfile.write(headers)

        for line in infile:
            fields = line.strip().split(',')
            if validate_row(fields):
                outfile.write(line)
            else:
                errfile.write(line)

def validate_row(fields):
    expected_num_fields = 6
    if len(fields) != expected_num_fields:
        return False
    try:
        datetime.strptime(fields[0], "%Y-%m-%d")
        datetime.strptime(fields[1], "%H:%M:%S")
        # Further checks can be added here
    except ValueError:
        return False

    return True






def CleanHash(filename):
    print('Cleaning ',filename)
    logging.warning('Cleaning '+filename)
    print (uniqCommand)
    type=filename[-3:len(filename)]
    if (type=="log"):
    
        OutputFileShort=open(Staging+'Outputshort.txt', 'a')
        OutputFileLong=open(Staging+'Outputlong.txt', 'a')

        InFile = open(RawFiles+filename,'r')
    
        Lines= InFile.readlines()
        for line in Lines:
            if (line[0]!="#"):
                Split=line.split(" ")
                
                if (len(Split)==14):
                   
                   OutputFileShort.write(line)
#                    print('Short ',filename,len(Split))
                else:
                   if (len(Split)==18):
                       OutputFileLong.write(line)
#                        print('Long ',filename,len(Split))
                   else:
                       print ("Fault "+str(len(Split)))
    
def DeleteFiles():
    OutputFileShort=open(Staging+'Outputshort.txt', 'w')
    OutputFileLong=open(Staging+'Outputlong.txt', 'w')

def ListFiles():
   
   arr=os.listdir(RawFiles)
   
   if not arr:
      print('List arr is empty')

# Output:
# 'List is empty'

   logging.warning('Starting List Files' +",".join(str(element) for element in arr)) 
   DeleteFiles()
   for f in arr:
       logging.warning('calling Clean '+f)
       CleanHash(f)
       
def BuildFactShort():
    InFile = open(Staging+'Outputshort.txt','r')
    OutFact1=open(Staging+'OutFact1.txt', 'a')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(" ")
        Browser=Split[9].replace(",","")
        Out=Split[0]+","+Split[1]+","+Browser+","+Split[8]+","+Split[13].replace("\n","")+","+Split[4] + "\n"

        OutFact1.write(Out)

def BuildFactLong():
    InFile = open(Staging+'Outputlong.txt','r')
    OutFact1=open(Staging+'OutFact1.txt', 'a')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(" ")
        Browser=Split[9].replace(",","")
        # 
        Out=Split[0]+","+Split[1]+","+Browser+","+Split[8]+","+Split[13].replace("\n","")+","+Split[4].replace("/", "") 
        OutFact1.write(Out)

def Fact1():
    with open(Staging+'OutFact1.txt', 'w') as file:
        file.write("Date,Time,Browser,IP,ResponseTime,RequestedFile\n")
    BuildFactShort()
    BuildFactLong()
 
def getIPs():
    InFile = open(Staging+'OutFact1.txt', 'r')
    OutputFile=open(Staging+'DimIP.txt', 'w')
    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(",")
        Out=Split[3]+"\n"
        OutputFile.write(Out)
def makeDimDate():
    InFile = open(Staging+'OutFact1.txt', 'r')
    OutputFile=open(Staging+'DimDate.txt', 'w')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(",")
        Out=Split[0]+"\n"
        OutputFile.write(Out)
 
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

 
def getDates():
    InDateFile = open(Staging+'DimDateUniq.txt', 'r')   
    OutputDateFile=open(StarSchema+'DimDateTable.txt', 'w')
    with OutputDateFile as file:
       file.write("Date,Year,Month,Day,DayofWeek\n")
    Lines= InDateFile.readlines()
    
    for line in Lines:
        line=line.replace("\n","")
        print(line)
        try:
            date=datetime.strptime(line,"%Y-%m-%d").date()
            weekday=Days[date.weekday()]
            out=str(date)+","+str(date.year)+","+str(date.month)+","+str(date.day)+","+weekday+"\n"
            
            with open(StarSchema+'DimDateTable.txt', 'a') as file:
               file.write(out)
        except:
            print("Error with Date")
            
def GetLocations():
    DimTablename=StarSchema+'DimIPLoc.txt'
    try:
        file_stats = os.stat(DimTablename)
    
        if (file_stats.st_size >2):
           print("Dim IP Table Exists")
           return
    except:
        print("Dim Table IP does not exist, creating one")
    InFile=open(Staging+'DimIPUniq.txt', 'r')
    OutFile=open(StarSchema+'DimIPLoc.txt', 'w')
    
    
    Lines= InFile.readlines()
    for line in Lines:
        line=line.replace("\n","")
        # URL to send the request to
        request_url = 'https://geolocation-db.com/jsonp/' + line
#         print (request_url)
        # Send request and decode the result
        try:
            response = requests.get(request_url)
            result = response.content.decode()
        except:
            print ("error reponse"+result)
        try:
        # Clean the returned string so it just contains the dictionary data for the IP address
            result = result.split("(")[1].strip(")")
        # Convert this data into a dictionary
            result  = json.loads(result)
            out=line+","+str(result["country_code"])+","+str(result["country_name"])+","+str(result["city"])+","+str(result["latitude"])+","+str(result["longitude"])+"\n"
#            print(out)
            with open(StarSchema+'DimIPLoc.txt', 'a') as file:
               file.write(out)
        except:
            print ("error getting location")

dag = DAG(                                                     
   dag_id="Process_W3_Data",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2023, 2, 24), 
   catchup=False,
)
download_data = PythonOperator(
   task_id="RemoveHash",
   python_callable=ListFiles, 
   dag=dag,
)

DimIp = PythonOperator(
    task_id="DimIP",
    python_callable=getIPs,
    dag=dag,
)

DateTable = PythonOperator(
    task_id="DateTable",
    python_callable=makeDimDate,
    dag=dag,
)

IPTable = PythonOperator(
    task_id="IPTable",
    python_callable=GetLocations,
    dag=dag,
)

BuildFact1 = PythonOperator(
   task_id="BuildFact1",
   python_callable= Fact1,
   dag=dag,
)

BuildDimDate = PythonOperator(
   task_id="BuildDimDate",
   python_callable=getDates, 
   dag=dag,
)

uniq = BashOperator(
    task_id="uniqIP",
    bash_command=uniqCommand,
#     bash_command="echo 'hello' > /home/airflow/gcs/Staging/hello.txt",

    dag=dag,
)

uniq2 = BashOperator(
    task_id="uniqDate",
    bash_command=uniqDateCommand,
#     bash_command="echo 'hello' > /home/airflow/gcs/Staging/hello.txt",

    dag=dag,
)

copyfact = BashOperator(
    task_id="copyfact",
#    bash_command=uniqDateCommand,
    bash_command="cp /opt/airflow/data/Staging/OutFact1.txt /opt/airflow/data/StarSchema/OutFact1.txt",

    dag=dag,
)


insert_data_task = PythonOperator(
    task_id="insert_data_into_db",
    python_callable=insert_data_into_db,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id="validate_and_clean_data",
    python_callable=validate_and_clean_data,
    dag=dag,
) 

# Update dependencies


  
# download_data >> BuildFact1 >>DimIp>>DateTable>>uniq>>uniq2>>BuildDimDate>>IPTable

download_data >> BuildFact1
BuildFact1 >> [DimIp, DateTable]
DimIp >> uniq
DateTable >> uniq2
uniq >> IPTable
uniq2 >> BuildDimDate
[IPTable, BuildDimDate] >> copyfact
copyfact >> validate_data_task
validate_data_task >> insert_data_task
