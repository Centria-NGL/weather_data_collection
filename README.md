# weather_data_collection
This repository includes the code used for collecting, formatting, testing and inserting weather data into an on premise SQL server.

One of the initial challenges in dealing with weather data was finding an optimal data source, balancing cost and quality of service, comparing them against many available options was a hurdle to overcome. At the end APIXU became inevitable because it provides interfaces in different programing languages. Demands were (1) hourly fetches of current weather state in specific cities as well (2) Daily fetches of forecasted weather data. All could be done with the chosen data provider.
Types of information stored in this area were divided in two groups:
-	Historic weather data
-	Live weather data

The data is spanned from first of April 2013 until first of April 2018. Each row represents values from one hour of a day so there are 24 rows for each day.
Live weather data – consist of current and forecast. As in historic data, current weather module stores information hourly with similar fields to that of historic data. About the forecast the only difference is that it is stored daily.
A complete guide to how APIXU is implemented can be found form their website but the code to meet requirements of ours will be presented. To bring updated values, collection processes need to be running constantly. The solution was to devote a computer in Centria’s SAP NextGen Lab to the collection and processing tasks which shall have minimum down time. Result of this was updated HANA tables containing current data of each city.
Requirements:
-	A call to weather data provider shall to be made every hour to receive the latest weather data for a specific location.
-	The data received has to be tested for correctness to avoid further damages in system.
-	Processes shall not stop in case of error and exceptions.
-	Report of issues shall be stored in log files.
-	Process of cleaning and preparing data for storage.
-	Ensuring storage is successful.
