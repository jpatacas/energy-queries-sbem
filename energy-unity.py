import pyarrow as pa
import pyarrow.parquet as pq
import json
from pyproj import Transformer

import sys

#weather file
weather_file = sys.argv[1]
# e.g. 'USA_CO_Denver.Internationa.725650_2018.epw'

#input json file
json_input = sys.argv[2]
# e.g. 'geojson/denver-station.json'

#output file name
file_name_json = sys.argv[3]
#e.g. 'json_output/denver-data-new2.json'

##########################################

#create buildings object
class Buildings(object):
    def toJson(self):

        return json.dumps(self, default=lambda o: o.__dict__) # wrong formatting - json.dump?

#call this on the input data
def features(data):
    #print(sid, data)

    data_array = [] #for the data to be emitted

    for elem in data:
        if (elem["type"] == "apartments"):
            data_array.append(df_res(elem, df_table_m2_res, weather_file))
        else:
            data_array.append(df_com(elem, df_table_com, weather_file))

    #need to add outer buildings object
    buildings = Buildings()
    buildings.buildings = data_array

    with open(file_name_json, 'w', encoding='utf-8') as f:
        json.dump(json.loads(buildings.toJson()), f, ensure_ascii=False, indent=4) # json.loads(buildings.toJson() - encode (toJson), and decode (json.loads)

table_res = pq.read_table('energydata/metadata-res.parquet') # load only columns to be used , columns=["col1"]...

df_table_res = pa.Table.to_pandas(table_res) # check data types

#convert total energy / elec / natural gas to kWh/m2 - resstock

df_table_m2_res = df_table_res.apply(lambda x : x * 10.76391 if (x.name == 'out.site_energy.total.energy_consumption_intensity') | (x.name == 'out.electricity.total.energy_consumption_intensity') | (x.name == 'out.natural_gas.total.energy_consumption_intensity' ) else x) # convert kwh/ft2/yr to kwh/m2/yr for the 'out.site_energy.total.energy_consumption_intensity' data column

table_com = pq.read_table('energydata/metadata-com.parquet')

df_table_com = pa.Table.to_pandas(table_com) # check data types

def df_com_simple(df_table_com, weather_file):

    #Denver - filter using weather file - in.weather_file_2018
    df_weather = df_table_com[(df_table_com['in.weather_file_2018'] == weather_file)]

    total_intensity = 0
    total_elec = 0
    total_naturalgas = 0

    if not df_weather.empty:
        total_intensity = df_weather['out.site_energy.total.energy_consumption'].values.mean() #/ feature["area"] #intensity = consumption / area
        total_elec = df_weather['out.electricity.total.energy_consumption'].values.mean() #/ feature["area"]
        total_naturalgas = df_weather['out.natural_gas.total.energy_consumption'].values.mean() #/ feature["area"]

        #'name': feature["name"],

    com_dict = {'type' : "com", 'total_intensity' : total_intensity, 'total_elec' : total_elec, 'total_naturalgas' : total_naturalgas}

    com_dict_json = json.dumps(com_dict)
    #print(com_dict_json)

  #  print(com_dict)

    return com_dict_json


def df_res_simple (df_table_res, weather_file):

    #deltaGifa = feature["area"] /10  # +- 10% of the initial area

    df_weather = df_table_res[(df_table_res['in.weather_file_2018'] == weather_file)] #looking at data for Denver

    # 2.	in.vacancy_status / 
    df_area = df_weather[(df_weather['in.vacancy_status'] == 'Occupied')]

    total_intensity = 0
    total_elec = 0
    total_naturalgas = 0

    if not df_area.empty: # still need to get rid of 0 values?
        total_intensity =  df_area['out.site_energy.total.energy_consumption_intensity'].values.mean()
        total_elec = df_area['out.electricity.total.energy_consumption_intensity'].values.mean()
        total_naturalgas = df_area['out.natural_gas.total.energy_consumption_intensity'].values.mean()

    res_dict = { 'type' : "res", 'total_intensity' : total_intensity, 'total_elec' : total_elec, 'total_naturalgas' : total_naturalgas}

    res_dict_json = json.dumps(res_dict)
    print(res_dict_json)

 # print to json file (each)
    return res_dict_json

# def print_json():
#     com_dict = df_com_simple(df_table_com)
#     res_dict = df_res_simple(df_table_m2_res)

#     with open('energy-com.json','w') as json_file:
#         json.dumps(com_dict, json_file)

#     with open('energy-res.json','w') as json_file:
#         json.dump(res_dict, json_file)

#print_json()


def df_com (feature, df_table_com, weather_file): #metrics are in consumption units

   # print(feature["housenumber"])
    area_sqft = feature["area"] * 10.76391

    deltaGifa = area_sqft /3  # +- 33% of the initial area, otherwise won't get enough results 

    #Denver - filter using weather file - in.weather_file_2018
    df_weather = df_table_com[(df_table_com['in.weather_file_2018'] == weather_file)]

    df_area = df_weather[(df_weather['in.sqft']  < area_sqft + deltaGifa) & (df_weather['in.sqft']  > area_sqft - deltaGifa)]

    #df_area = df_weather

    total_intensity = 0
    total_elec = 0
    total_naturalgas = 0

    if not df_area.empty:
        total_intensity = df_area['out.site_energy.total.energy_consumption'].values.mean() / feature["area"] #intensity = consumption / area
        total_elec = df_area['out.electricity.total.energy_consumption'].values.mean() / feature["area"]
        total_naturalgas = df_area['out.natural_gas.total.energy_consumption'].values.mean() / feature["area"]

        #'name': feature["name"],

    com_dict = { 'id' : feature["id"], 'name': feature.get("name"), 'housenumber': feature.get("housenumber"), 'street' : feature.get("street"), 'area' : feature["area"], 'type' : feature["type"], 'total_intensity' : total_intensity, 'total_elec' : total_elec, 'total_naturalgas' : total_naturalgas}

    #print(com_dict)

    return com_dict

def df_res(feature, df_table_m2, weather_file): #metrics are in intensity units

    deltaGifa = feature["area"] /10  # +- 10% of the initial area

    df_weather = df_table_m2[(df_table_m2['in.weather_file_2018'] == weather_file)] #looking at data for Denver

    # 2.	in.vacancy_status / 
    dfvacancy = df_weather[(df_weather['in.vacancy_status'] == 'Occupied')]

    df_area = dfvacancy[(dfvacancy['in.floor_area_conditioned_ft_2']  < feature["area"] + deltaGifa) & (dfvacancy['in.floor_area_conditioned_ft_2']  > feature["area"] - deltaGifa)]

    total_intensity = 0
    total_elec = 0
    total_naturalgas = 0

    if not df_area.empty: # still need to get rid of 0 values?
        total_intensity =  df_area['out.site_energy.total.energy_consumption_intensity'].values.mean()
        total_elec = df_area['out.electricity.total.energy_consumption_intensity'].values.mean()
        total_naturalgas = df_area['out.natural_gas.total.energy_consumption_intensity'].values.mean()

    res_dict = { 'id' : feature["id"], 'name': feature.get("name"), 'housenumber': feature.get("housenumber"), 'street' : feature.get("street"), 'area' : feature["area"], 'type' : feature["type"], 'total_intensity' : total_intensity, 'total_elec' : total_elec, 'total_naturalgas' : total_naturalgas}

    #print(res_dict)

    return res_dict

# energy metrics com (kWh) / res (kWh/sqft)
# res: 'out.site_energy.total.energy_consumption_intensity' / out.electricity.total.energy_consumption_intensity / out.natural_gas.total.energy_consumption_intensity / out.propane.total.energy_consumption_intensity / out.fuel_oil.total.energy_consumption_intensity / out.wood.total.energy_consumption_intensity ...
# com: out.site_energy.total.energy_consumption / out.electricity.total.energy_consumption / out.natural_gas.total.energy_consumption / out.other_fuel.total.energy_consumption / out.district_cooling.total.energy_consumption / out.district_heating.total.energy_consumption

# python3 program to evaluate
# area of a polygon using
# shoelace formula

# (X[i], Y[i]) are coordinates of i'th point.
def polygonArea(X, Y, n):

	# Initialize area
	area = 0.0

	# Calculate value of shoelace formula
	j = n - 1
	for i in range(0,n):
		area += (X[j] + X[i]) * (Y[j] - Y[i])
		j = i # j is previous vertex to i
	

	# Return absolute value
	return int(abs(area / 2.0))

# This code is contributed by
# Smitha Dinesh Semwal

def calcAreaGeoJson (coordArray): # ok

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857")

    geoJsonArea = 0

    for i in range(len(coordArray)):
        poly = coordArray[i]

        xcoord = []
        ycoord = []

        if isinstance(poly, list): # check datatype 

            for k in range(len(poly) -1):

                pointConv = transformer.transform(poly[k][1], poly[k][0]) # long lat are switched
                xcoord.append(pointConv[0])
                ycoord.append(pointConv[1])

                #print(xcoord, ycoord)

            geoJsonArea += polygonArea(xcoord, ycoord, len(xcoord))

            return geoJsonArea

def getIdAreaGeoJson (features): # returns array of objects with { "id": cleanId, "area": area, "type": type} for geoJson files from overpass API 

    geoObjArray = []

    for i in range(len(features)):

        #print(features[i])
    
        id = features[i]['id']
        type = features[i]['properties'].get("building")
        name = features[i]['properties'].get("name")
        housenumber = features[i]['properties'].get("addr:housenumber")
        street = features[i]['properties'].get("addr:street")


        #if id string not includes "node" - remove node elements (area = 0)
        if 'node' not in id:

            coordsArray = features[i]['geometry']['coordinates']
            area = calcAreaGeoJson(coordsArray)

            geoObj = { "id": id, "name": name, "housenumber": housenumber, "street": street, "area": area, "type": type}
                
            geoObjArray.append(geoObj)

    return geoObjArray #//do somthing with it
 
# Opening JSON file
f = open(json_input)
  
# returns JSON object as 
# a dictionary
data = json.load(f)

#coordinates = []

areas_data = getIdAreaGeoJson(data['features'])

#filter by name?


filtered_by_name = []

# buildings: Coors Field / 1144 Fifteenth / One Tabor Center / 1801 California Street Building / Wells Fargo Center / Republic Plaza Building

building_names = ["Coors Field","1144 Fifteenth","One Tabor Center","1801 California Street Building","Wells Fargo Center","Republic Plaza Building"]

for elem in areas_data:
    for building_name in building_names:
        if elem.get("name") == building_name:
            filtered_by_name.append(elem)


print(filtered_by_name)
#features(areas_data)
features(filtered_by_name)
  
# Closing file
f.close()
