# Created by Markus Kaefer
import socket
import mysql.connector
import json
import datetime
import time
import logging

# Importing credentials and variables
from variables import *

# creating logger object
logger = logging.getLogger("updserver")
logger.setLevel(logging.DEBUG)
# create a file handler
logfile = logging.FileHandler('/var/log/udpserver.log')
logfile.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logfile.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(logfile)


def main():
    sock=UDP_create_socket()
    while True:
        rcv_data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
        rcv_string = rcv_data.decode("utf-8")
        #print "received message: ", rcv_string
        logger.info("received message: " + rcv_string)
        select_parser(rcv_data)


def UDP_create_socket():
    logger.info("Trying to create socket on: " + UDP_IP + ":" + str(UDP_PORT))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    sock.bind((UDP_IP, UDP_PORT))
    if not sock:
        logger.error("ERROR Binding socket - " + UDP_IP + ":" + str(UDP_PORT))
    elif sock:
        logger.info("Bound to socket: " + UDP_IP + ":" + str(UDP_PORT))
    return sock


def DB_connect():
    connection = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD, database=MYSQL_DB)
    if connection:
        logger.info("Connected to MySQL Server " + MYSQL_HOST + " with DB " + MYSQL_DB )
    elif not connection:
        logger.error("Could not connect to database! ("+ MYSQL_DB + " on " + MYSQL_HOST+ ")")
    return connection

def select_parser(rcv_data):
    if not rcv_data or "TCH-ERR:" in rcv_data or "TCH-OK:" in rcv_data:
        logger.warn("received wrong repsonse - but caught it... --- " + rcv_data)
        return

    noJson = False

    try: 
        rcv_json = json.loads(rcv_data)
    except:
        logger.warn("Data received is no JSON... ---" + rcv_data)
        noJson = True
        return

    if "Session ID" in rcv_json and noJson == False:
        parse_report(rcv_json)
    elif "E pres" in rcv_json and noJson == False:
        parse_broadcast(rcv_json)

def parse_broadcast(jsonData):
    logger.debug("Clearing broadcast parsing fields.")

    #sState = None
    #sPlug = None
    #sInput = None
    #sEnableSys = None
    #sMaxCurr = None
    sEpres_RAW = 0.0
    sEpres = 0.0
    
    #sState = jsonList["State"] 
    #sPlug = jsonList["Plug"]
    #sInput = jsonList["Input"]
    #sEnableSys = jsonList["Enable sys"]
    #sMaxCurr = jsonList["Max curr"]
    
    sEpres_RAW = jsonData["E pres"]  
    # Convert value to Watt-hours
    logger.info("Power Value: " + str(sEpres_RAW))
    sEpres = float(sEpres_RAW / 10)
    logger.info("Power Value: " + str(sEpres))

    logger.info("Reading Data - Broadcast: " + " E pres: " + str(sEpres))
        #----- State: " + str(sState) + " , Plug: " + str(sPlug) 
        #      + " Input: " + str(sInput) + " Enable sys: " + str(sEnableSys) + " Max curr: " + str(sMaxCurr) 

    sNow = datetime.datetime.now()
    #sNowDateTime = datetime.datetime.strptime(sNow, "%Y-%m-%d %H:%M:%S.%f")
    sNowDate = sNow.date()
    sNowTime = sNow.time()
    # Converting Start Datetime to Unix Timestamp (for Grafana Graphs)
    #sDateTimeN = datetime.datetime(sNow, "%Y-%m-%d %H:%M:%S.%f")   
    sNowTimestamp = time.mktime(sNow.timetuple())

    connection=DB_connect()
    cursor = connection.cursor()
    insertBroadcastSQL = """INSERT INTO charging (c_date, c_time, c_timestamp, c_E_pres) 
                                         VALUE (%s, %s, %s, %s) """
    cursor.execute(insertBroadcastSQL, (sNowDate, sNowTime, sNowTimestamp, sEpres))
    connection.commit()
    logger.info("Inserating into Database into Table charging: " + "Date: " + str(sNowDate) + ", Time: " + str(sNowTime)
              + ", NowTimestamp: " + str(sNowTimestamp) + ", E-Pres: " + str(sEpres) )
    cursor.close()
    del cursor

    if connection.is_connected():
        connection.close() 
        logger.info("Closed Databse connection.")



def parse_report(jsonData):
    logger.debug("Clearing report parsing fields.")

    dbrsfield = None
    sSessionID = None
    sEstart = None
    sEpres_RAW = 0.0
    sEpres = 0.0
    sStarted = None
    sEnded = None
    sReason = None
    sEndReason = None
    sRFIDTag = None
    sDateTimeS = None
    sDateTimeE = None
    sStartDate = None
    sStartTime = None
    sStartTimestamp = None
    sEndDate = None
    sEndTime = None
    sEndTimestamp = None
    sRFIDTag = None
    sUser = None
    
    sSessionID = jsonData["Session ID"] 
    sEstart = jsonData["E start"]
    sEpres_RAW = jsonData["E pres"]  
    sStarted = jsonData["started"]
    sEnded = jsonData["ended"]
    sReason = jsonData["reason"]
    sRFIDTag = jsonData["RFID tag"]
    
    logger.info("Reading Data - Report: ----- SessionID: " + str(sSessionID) + " , E-Start: " + str(sEstart) 
              + " E-Pres: " + str(sEpres_RAW) + " started: " + str(sStarted) + " ended: " + str(sEnded) 
              + " reason: " + str(sReason) + " RFID-Tag: " + str(sRFIDTag) )

    #"ID": "101",
    #"Session ID": 100,
    #"Curr HW": 16000,
    #"E start": 12589896,
    #"E pres": 126722,
    #"started[s]": 1551996481,
    #"ended[s]": 1552025482,
    #"started": "2019-03-07 22:08:01.000",
    #"ended": "2019-03-08 06:11:22.000",
    #"reason": 1,
    #"timeQ": 0,
    #"RFID tag": "058ae8d183a00000",
    #"RFID class": "00000000000000000000",
    #"Serial": "18690130",
    #"Sec": 9114914

    # Creating Database Connection
    connection=DB_connect()

    # Convert value to Watt-hours
    logger.info("Power Value: " + str(sEpres_RAW))
    sEpres = float(sEpres_RAW / 10)
    logger.info("Power Value: " + str(sEpres))

    # Converting Start Datetime to Date and Time
    sDateTimeS = datetime.datetime.strptime(sStarted, "%Y-%m-%d %H:%M:%S.%f")
    sStartDate = sDateTimeS.date()
    sStartTime = sDateTimeS.time()
    # Converting Start Datetime to Unix Timestamp (for Grafana Graphs)
    # sDateTimeS = datetime.datetime(sStarted, "%Y-%m-%d %H:%M:%S.%f")   
    sStartTimestamp = time.mktime(sDateTimeS.timetuple())
    
    # Converting End Datetime to Date and Time
    try: 
        sDateTimeE = datetime.datetime.strptime(sEnded, "%Y-%m-%d %H:%M:%S.%f")
        sEndDate = sDateTimeE.date()
        sEndTime = sDateTimeE.time()
        # Converting Start Datetime to Unix Timestamp (for Grafana Graphs)
        #sDateTimeE = datetime.datetime(sEnded, "%Y-%m-%d %H:%M:%S.%f")   
        sEndTimestamp = time.mktime(sDateTimeE.timetuple())
    except: 
        logger.info("Current session running, no end datetime")
        sEndDate = None
        sEndTime = None
        sEndTimestamp = None

    # getting User ID with RFID Tag ID
    cursor = connection.cursor()
    getUserSQL = "SELECT u_key_id FROM user WHERE u_user_id=%s"
    cursor.execute(getUserSQL, (sRFIDTag,))
    result = cursor.fetchall()
    rowcount = cursor.rowcount

    if rowcount == 1:
        for field in result:
            sUser = field[0]
            logger.debug("Getting UserID.")

    if rowcount > 1:
        sUser = 0
        logger.error("DATA ERROR - more than one user information found?!? --- Rowcount: " + str(rowcount))

    if rowcount < 1:
        logger.error("DATA ERROR - no user information found! --- Rowcount: " + str(rowcount))
        sUser = 0

    rowcount = None
    result = None
    field = None
    cursor.close()
    del cursor

    # Getting reason for end of session
    cursor = connection.cursor()
    getEndReasonSQL = "SELECT r_key_id FROM reasons WHERE r_value_reason=%s"
    cursor.execute(getEndReasonSQL, (sReason,))
    result = cursor.fetchall()
    rowcount = cursor.rowcount
    if rowcount == 1:
        for field in result:
            sEndReason = field[0]

    if rowcount > 1:
        logger.error("DATA ERROR - more than one EndReason information found?!? --- Rowcount: " + str(rowcount))
        sEndReason = None

    if rowcount < 1:
        logger.error("DATA ERROR - no EndReason Information found?!? --- Rowcount: " + str(rowcount))
        sEndReason = None

    rowcount = None
    result = None
    field = None
    cursor.close()
    del cursor

    # Check if Report is already in DB
    cursor = connection.cursor()
    getReportSQL = "SELECT s_session_id FROM sessions WHERE s_session_id = %s"
    cursor.execute(getReportSQL, (sSessionID,))
    result = cursor.fetchall()
    rowcount = cursor.rowcount

    if rowcount > 1 or rowcount == None:
        logger.error("DATA ERROR - while creating session found more sessions with the same Sesssion ID --- Rowcount: " + rowcount)
    
    if rowcount == 0:
    # if no the INSERT a new row into the database
        cursor = connection.cursor()
        insertReportSQL = """INSERT INTO sessions (s_session_id, s_E_start, s_E_pres, s_started_date, s_starttime, s_start_timestamp, 
                                             s_end_date, s_endtime, s_end_timestamp, s_end_reason, s_user) 
                                             VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
        cursor.execute(insertReportSQL, (sSessionID, sEstart, sEpres, sStartDate, sStartTime, sStartTimestamp, sEndDate, sEndTime, 
                                         sEndTimestamp, sEndReason, sUser, ))
        connection.commit()
        logger.info("Inserating into Database into Table sessions: " + "SessionID: " + str(sSessionID) + ", E-Start: " + str(sEstart)
                  + ", E-Pres: " + str(sEpres) + ", StartDate: " + str(sStartDate) + ", Starttime: " + str(sStartTime)
                  + ", Starttimestamp: " + str(sStartTimestamp) + ", EndDate: " + str(sEndDate) + ", Endtime: " + str(sEndTime) 
                  + ", Endtimestamp: " + str(sEndTimestamp) + ", Reason: " + str(sEndReason)
                  + ", User: " + str(sUser) )
    if rowcount == 1:
    # if yes the UPDATE it
        cursor = connection.cursor()
        updateReportSQL = "UPDATE sessions SET s_E_pres=%s, s_end_date=%s, s_endtime=%s, s_end_timestamp=%s, s_end_reason=%s WHERE s_session_id = %s"
        cursor.execute(updateReportSQL, (sEpres, sEndDate, sEndTime, sEndTimestamp, sEndReason, sSessionID, ))
        connection.commit()
        logger.info("Updating Database - Table sessions: " + "SessionID: " + str(sSessionID) + ", E-Pres: " + str(sEpres) 
                  + ", EndDate: " + str(sEndDate) + ", Endtime: " + str(sEndTime) + ", Endtimestamp: " + str(sEndTimestamp) + ", Reason: " + str(sEndReason) )

    cursor.close()
    del cursor

    if connection.is_connected():
        connection.close() 
        logger.info("Closed Databse connection.")


main()