//TaskA

//1.
//mongoimport --db fit5148_db --collection fire --type csv --headerline --ignoreBlanks --file /Users/mac/Desktop/5148/Assi2/FireData-Part1.csv

//mongoimport --db fit5148_db --collection climate --type csv --headerline --ignoreBlanks --file /Users/mac/Desktop/5148/Assi2/ClimateData-Part1.csv

//2.
use fit5148_db

db.climate.find({"Date":"2017-12-15"})

//3.
db.fire.find({"Surface Temperature (Celcius)": {$gte: 65, $lte:100}},{"Latitude":1,"Longitude":1,"Confidence":1}).pretty()

//4.
db.climate.aggregate({ $lookup:     {         from: "fire",         localField: "Date",         foreignField : "Date"     ,as: "climate_fire"     }},{$unwind: "$climate_fire"},  {$match : {"Date": {$gte: "2017-12-15", $lte: "2017-12-16"}}}, {$project: {"_id": 0,  "Air Temperature(Celcius)":1, "Relative Humidity":1, "Max Wind Speed":1, "Date":1,  "SurfaceTem" : "$climate_fire.Surface Temperature (Celcius)" }} ).pretty()

//5.
db.fire.aggregate({ $lookup:     {         from: "climate",         localField: "Date",         foreignField : "Date"     ,as: "fire_climate"     }},{$unwind: "$fire_climate"},  {$match : {"Confidence": {$gte: 80, $lte: 100}}}, {$project: {"_id": 0, "Confidence":1, "Surface Temperature (Celcius)":1, "Datetime":1, "AirTem" : "$fire_climate.Air Temperature(Celcius)"   }} ).pretty()

//6.
db.fire.find({}).sort({"Surface Temperature (Celcius)": -1}).limit(10)

//7.
db.fire.aggregate({$group:{ _id: {date: "$Date"},count:{$sum:1}}})

//8.
db.fire.aggregate({$group:{ _id: {date: "$Date"},avgSurfaceTem:{$avg: "$Surface Temperature (Celcius)"}}})


mongo < /Users/mac/Desktop/as2TaskA.js >> /Users/mac/Desktop/output.txt




db.climatefire.find({"Air Temperature(Celcius)":16},{climate_fire:1})

db.climatefire.aggregate(
{$unwind: "$climate_fire"},
{$match: {"Air Temperature(Celcius)":16}}, {$project:{"climate_fire":1}}
)


db.climatefire.aggregate( {$unwind:"$climate_fire"},
{$match:{"climate_fire.Date":"2017-11-30","climate_fire.Power":{$gt:100}}}
)

db.climatefire.aggregate( {$match:{"climate_fire":{$elemMatch:{"Date":"2017-11-30","Power":{$gt:100}}}}}, {$unwind:"$climate_fire"}, {$match:{"climate_fire.Date":"2017-11-30","climate_fire.Power":{$gt:100}}}
)
