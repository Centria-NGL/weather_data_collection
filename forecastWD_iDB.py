import pypyodbc, time, logging, threading
from weatherData import reqWeatherData, forecastWeatherData_3rd_Day

logging.basicConfig(filename= 'log_WD_process', level = logging.DEBUG,
                    format = '(%(asctime)s) %(levelname)s : %(message)s')
REQ_INTERVALS = 86400
db_host = 'YOUR DATABASE IP ADDRESS'
db_name = 'DATABASE NAME AS IN MSSQL'
db_user = 'USERNAME'
db_password = 'PASSWORD'
connection_string = 'Driver={SQL Server};Server=' + db_host + ';Database=' + db_name + ';UID=' + db_user + ';PWD=' + db_password + ';'

SQLCommand_forecastWeather = ("INSERT INTO WEATHER_FORECAST"
                "(FORECAST_DATE, FORECAST_TIME, TEMPERATURE, HUMIDITY, WINDSPEED, RAIN, SNOW) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)")


def insertData(SQLCommand, values):
    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()
    for data in values.values():
        cursor.execute(SQLCommand, data)
        connection.commit()
    connection.close()

def collectNonStop():
    threading.Timer(REQ_INTERVALS, collectNonStop).start()
    logging.info("STARTING FORECAST WEATHER DATA COLLECTION")
    response    = reqWeatherData()
    forecast_3Days = forecastWeatherData_3rd_Day(response)
    if not forecast_3Days:
        logging.warning('forecast Data fetching faced issue, continue in 10 seconds')
        time.sleep(10)
        collectNonStop()
    else:
        try:
            insertData(SQLCommand_forecastWeather, forecast_3Days)
        except Exception as e:
            logging.exception("failed to insert forecast data into database")
            time.sleep(10)
            collectNonStop()
        else:
            logging.info("inserted forecast weather data")

collectNonStop()
