from apixu.client import ApixuClient, ApixuException
import time, datetime, logging
from dateutil import tz


logging.basicConfig(filename= 'weatherDataLog', level = logging.DEBUG,
                    format = '(%(asctime)s) %(levelname)s : %(message)s')
api_key_Silver = 'YOURKEY'
api_key = 'YOURKEY'
client = ApixuClient(api_key)


# fetch data from APIXU
def reqWeatherData():
    rawData = dict()
    try:
        rawData = client.getForecastWeather(q='Helsinki', days = 4)
    except Exception as e:
        logging.warning(e)
        time.sleep(900)
        reqWeatherData()
    else:
        return rawData


# process current weather data
def currentWeatherData(rawData):
    data = list()
    try:
        date_time = rawData['current']['last_updated'].split(' ')
        date = date_time[0]
        time = date_time[1]
        data.append(date)
        data.append(time)
        data.append(rawData['current']['temp_c'])
        data.append(rawData['current']['pressure_mb'])
        data.append(rawData['current']['humidity'])
        data.append(rawData['current']['wind_kph'])
        data.append(rawData['current']['cloud'])
        percip = rawData['current']['precip_mm']
        condition = rawData['current']['condition']['text']
        if "snow" in condition:
            data.append("0.0")
            data.append(percip)
        else:
            data.append(percip)
            data.append("0.0")
    except:
        logging.exception("" ,rawData['current'])
        return data
    else:
        data.append(1)
        return data


# process forecast weather data for following 3 days
def forecastWeatherData_3_days(rawData):
        data = dict()
        time = datetime.datetime.now(tz.gettz("Europe/Helsinki")).strftime("%H:%M:%S")
        try:
            day = rawData['forecast']['forecastday']
            for d in day:
                data[d['date']] = list()
                data[d['date']] = [d['date'], time,
                d['day']['avgtemp_c'], d['day']['avghumidity'],
                d['day']['maxwind_kph']]

                percip = d['day']['totalprecip_mm']
                condition = d['day']['condition']['text']
                if "snow" in condition:
                    data[d['date']].append("0.0")
                    data[d['date']].append(percip)
                else:
                    data[d['date']].append(percip)
                    data[d['date']].append("0.0")
        except:
            logging.exception("Forecast conversion failed")
            return data
        else:
            return data



#  process forecast weather data for 3rd day
def forecastWeatherData_3rd_Day(rawData):
    data = dict()
    try:
        day = rawData['forecast']['forecastday'][-1]
        date = day['date']
        time = datetime.datetime.now(tz.gettz("Europe/Helsinki")).strftime("%H:%M:%S")

        data[date] = [date, time,
        day['day']['avgtemp_c'],
        day['day']['avghumidity'],
        day['day']['maxwind_kph']]

        percip = day['day']['totalprecip_mm']
        condition = day['day']['condition']['text']
        if "snow" in condition:
            data[date].append("0.0")
            data[date].append(percip)
        else:
            data[date].append(percip)
            data[date].append("0.0")

    except:
        logging.exception("Forecast conversion failed")
        return data
    else:
        return data

def convert_epoch(time_epoch):
    NYC = tz.gettz('America/New_York')
    str_value = datetime.datetime.fromtimestamp(time_epoch, NYC).strftime("%Y-%m-%d %H:00:00")
    return str_value
