Baseline Scenario:

The Freight Analysis Framework (FAF) integrates data from a variety of sources to create a comprehensive national picture of
freight movements among states and major metropolitan areas by all modes of transportation. It provides a national picture of current freight flows to, from, 
and within the United States, assigns the flows to the transportation network, and projects freight flow patterns into the future.
The FAF flow matrix described in this report is to forecast future freight shipment weights .
The FAF Forecasts must to be driven by the most up to date macroeconomic assumptions on the short- and long-term trends of the United States .

Challenge addressed by the use case:

It's unavailable to get forecasted economic dataSet by industry sector and by geographic region to forecast growth rate for long term (from 2020 to 2040) .
To solve this I assumed to start from the end which it mean to get the final results from the available dataSets ( in our case the goods in tons from 2012 to 2018 )
to calculate the expected growth rate in goods tons between each year and the base year ( 2012 ) .
To calculate the growth rate for the next years in short term (from 2019 to 2024) from the available dataSets ,
I assumed to calculate the growth rate for each year and the previous year for it and get the avarage for all of these growth rates (2012 - 2018) 
and added it to next year such as 2019 and calculate the growth rate for 2019 based on 2012.
This process will repeat to 2024 .

My Goal:

The goal is to forecast freight shipment weights in 2024 .

The Model:

The dataSet from 2012 - 2018 got divided in term of domestic origin to 22 model which each model contains 6 origin out of 132 domestic origin.

The Deployment:

Used RandomForestRegressor algorithm to apply it in spark standalone cluster.
