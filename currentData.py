import pypyodbc
from weatherData import reqWeatherData, currentWeatherData
import time
import logging


logging.basicConfig(filename= 'weatherDataLog', level = logging.DEBUG)

db_host = 'YOUR DATABASE IP ADDRESS'
db_name = 'DATABASE NAME AS IN MSSQL'
db_user = 'USERNAME'
db_password = 'PASSWORD'

connection_string = 'Driver={SQL Server};Server=' + db_host + ';Database=' + db_name + ';UID=' + db_user + ';PWD=' + db_password + ';'

SQLCommand_currentWeather = ("INSERT INTO WEATHER_CURRENT"
                "(LAST_UPDATED, LAST_UPDATED_TIME, TEMPERATURE, PRESSURE, HUMIDITY, WINDSPEED, CLOUDINESS, PRECIPITATION) "
                "VALUES (?,?,?,?,?,?,?,?)")



def insertData(SQLCommand, values):
    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor.execute(SQLCommand, values)
    connection.commit()
    connection.close()



def collectNonStop():
    response    = reqWeatherData()
    currentData = currentWeatherData(response)
    if not currentData:
        logging.warning('current Data fetching faced issue, continue in 10 seconds')
        time.sleep(10)
        collectNonStop()
    else:
        try:
            insertData(SQLCommand_currentWeather, currentData)
        except Exception as e:
            logging.exception("failed to insert current data into database")
            time.sleep(10)
            collectNonStop()
        else:
            logging.info("inserted current weather data")
            time.sleep(3600)
