import pypyodbc
from weatherData import reqWeatherData,  forecastWeatherData_3_days, forecastWeatherData_3rd_Day
import time
from currentData import insertData
import logging

logging.basicConfig(filename= 'forecastDataLog', level = logging.DEBUG)

db_host = 'YOUR DATABASE IP ADDRESS'
db_name = 'DATABASE NAME AS IN MSSQL'
db_user = 'USERNAME'
db_password = 'PASSWORD'

SQLCommand_forecastWeather = ("INSERT INTO WEATHER_FORECAST"
                "(FORECAST_TIME, FORECAST_UPDATED_TIME, TEMPERATURE, PRESSURE, HUMIDITY, WINDSPEED, CLOUDINESS, PRECIPITATION) "
                "VALUES (?,?,?,?,?,?,?,?)")

connection_string = 'Driver={SQL Server};Server=' + db_host + ';Database=' + db_name + ';UID=' + db_user + ';PWD=' + db_password + ';'


def firstCollection():
    response = reqWeatherData()
    firstForecast = forecastWeatherData_3_days(response)
    try:
        connection = pypyodbc.connect(connection_string)
        cursor = connection.cursor()
        for day in firstForecast:
            cursor.execute(SQLCommand_forecastWeather, day)
            connection.commit()
        connection.close()
    except Exception as e:
        logging.exception('failed to collect first 3 days forecast, next try in 20 seconds.')
        time.sleep(20)
        firstCollection()
    else:
        logging.info('forecast of first 3 days collected')
        collectNonStop()


def collectNonStop():
    response = reqWeatherData()
    forecastData = forecastWeatherData_3rd_Day(response)
    if not forecastData:
        logging.warning('forecast Data fetching faced issue, continue in 10 seconds')
        time.sleep(10)
        collectNonStop()
    else:
        try:
            insertData(SQLCommand_forecastWeather, forecastData)
            logging.info('forecast of 3rd day collected. next round in 3 days.')
        except Exception as e:
            logging.exception('failed to insert forecast into tables.')
            time.sleep(20)
        else:
            time.sleep(259200)
