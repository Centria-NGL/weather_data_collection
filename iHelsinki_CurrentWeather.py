import pypyodbc, time, logging, threading
from helsinkiWeather import reqWeatherData, currentWeatherData

REQ_INTERVALS = 3600
logging.basicConfig(filename= 'WD_HEL_process.log', level = logging.DEBUG,
                    format = '(%(asctime)s) %(levelname)s : %(message)s')

db_host = 'YOUR DATABASE IP ADDRESS'
db_name = 'DATABASE NAME AS IN MSSQL'
db_user = 'USERNAME'
db_password = 'PASSWORD'

connection_string = 'Driver={SQL Server};Server=' + db_host + ';Database=' + db_name + ';UID=' + db_user + ';PWD=' + db_password + ';'

SQLCommand_currentWeather = ("INSERT INTO WEATHER_CURRENT_HELSINKI"
                "(LAST_UPDATED_DATE, LAST_UPDATED_TIME, TEMPERATURE, PRESSURE, HUMIDITY, WINDSPEED, CLOUDINESS, RAIN, SNOW, IS_CURRENT) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)")

set_current_zero_command_sql = ("UPDATE WEATHER_CURRENT_HELSINKI "
                               "SET IS_CURRENT = ? "
                               " WHERE LAST_UPDATED_TIME = ?")

retrieve_last_updated_time = "SELECT TOP 1 * FROM WEATHER_CURRENT_HELSINKI ORDER BY ROWID DESC"

def insertData(SQLCommand, values):
    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()
    LAST_REPORTED_TIME = list()
    cursor.execute(retrieve_last_updated_time)
    row = cursor.fetchone()
    if row:
        LAST_REPORTED_TIME.append(0)
        LAST_REPORTED_TIME.append(row[-9])
        cursor.execute(set_current_zero_command_sql, LAST_REPORTED_TIME)
        connection.commit()
    cursor.execute(SQLCommand, values)
    connection.commit()
    connection.close()

def collectNonStop():
    threading.Timer(REQ_INTERVALS, collectNonStop).start()
    logging.info("STARTING CURRENT WEATHER DATA COLLECTION")
    response    = reqWeatherData()
    currentData = currentWeatherData(response)
    if not currentData:
        logging.warning('current Data fetching faced issue, continue in 10 seconds')
        time.sleep(900)
        collectNonStop()
    else:
        try:
            insertData(SQLCommand_currentWeather, currentData)
        except Exception as e:
            logging.exception("failed to insert current data into database")
            time.sleep(900)
            collectNonStop()
        else:
            logging.info("inserted current weather data")

collectNonStop()
