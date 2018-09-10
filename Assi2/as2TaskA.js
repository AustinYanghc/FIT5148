//TaskA

//1.Import the data (Fire data-Part 1 and Weather data-Part 1) into two different collections in MongoDB.

//mongoimport --db fit5148_db --collection fire --type csv --headerline --ignoreBlanks --file /Users/mac/Desktop/5148/Assi2/FireData-Part1.csv

//mongoimport --db fit5148_db --collection climate --type csv --headerline --ignoreBlanks --file /Users/mac/Desktop/5148/Assi2/ClimateData-Part1.csv

//2.Find climate data on 15th December 2017.
use fit5148_db

db.climate.find({"Date":"2017-12-15"})

//3.Find the latitude, longitude and confidence when the surface temperature (°C) was between 65 °C and 100 °C.
db.fire.find({"Surface Temperature (Celcius)": {$gte: 65, $lte:100}},{"Latitude":1,"Longitude":1,"Confidence":1}).pretty()

//4.Find surface temperature (°C), air temperature (°C), relative humidity and maximum wind speed on 15th and 16th of December 2017.
db.climate.aggregate({ $lookup:     {         from: "fire",         localField: "Date",         foreignField : "Date"     ,as: "climate_fire"     }},{$unwind: "$climate_fire"},  {$match : {"Date": {$gte: "2017-12-15", $lte: "2017-12-16"}}}, {$project: {"_id": 0,  "Air Temperature(Celcius)":1, "Relative Humidity":1, "Max Wind Speed":1, "Date":1,  "SurfaceTem" : "$climate_fire.Surface Temperature (Celcius)" }} ).pretty()

//5.Find datetime, air temperature (°C), surface temperature (°C) and confidence when the confidence is between 80 and 100.
db.fire.aggregate({ $lookup:     {         from: "climate",         localField: "Date",         foreignField : "Date"     ,as: "fire_climate"     }},{$unwind: "$fire_climate"},  {$match : {"Confidence": {$gte: 80, $lte: 100}}}, {$project: {"_id": 0, "Confidence":1, "Surface Temperature (Celcius)":1, "Datetime":1, "AirTem" : "$fire_climate.Air Temperature(Celcius)"   }} ).pretty()

//6.Find top 10 records with highest surface temperature (°C).
db.fire.find({}, {"Surface Temperature (Celcius)":1, "_id":0}).sort({"Surface Temperature (Celcius)": -1}).limit(10)

//7.Find the number of fire in each day. You are required to only display total number of fire
and the date in the output.
db.fire.aggregate({$group:{ _id: {date: "$Date"},count:{$sum:1}}})

//8.Find the average surface temperature (°C) for each day. You are required to only display average surface temperature (°C) and the date in the output.
db.fire.aggregate({$group:{ _id: {date: "$Date"},avgSurfaceTem:{$avg: "$Surface Temperature (Celcius)"}}})


