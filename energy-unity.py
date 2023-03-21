import pyarrow as pa
import pyarrow.parquet as pq
import json
from pyproj import Transformer
import sys

##########################################
#inputs

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

        return json.dumps(self, default=lambda o: o.__dict__)
    
#call this on the input data, use filter_by_name function to get data only for specific buildings in the OSM dataset (geojson input file)
def features(data):

    data_array = []

    for elem in data:
        if (elem["type"] == "apartments"):
            data_array.append(df_res(elem, df_table_m2_res, weather_file))
        else:
            data_array.append(df_com(elem, df_table_com, weather_file))

    #create buildings object
    buildings = Buildings()
    buildings.buildings = data_array

    with open(file_name_json, 'w', encoding='utf-8') as f:
        json.dump(json.loads(buildings.toJson()), f, ensure_ascii=False, indent=4) # json.loads(buildings.toJson() - encode (toJson), and decode (json.loads)

table_res = pq.read_table('energydata/metadata-res.parquet')

df_table_res = pa.Table.to_pandas(table_res)

#convert total energy / elec / natural gas to kWh/m2 - ResStock
df_table_m2_res = df_table_res.apply(lambda x : x * 10.76391 if (x.name == 'out.site_energy.total.energy_consumption_intensity') | (x.name == 'out.electricity.total.energy_consumption_intensity') | (x.name == 'out.natural_gas.total.energy_consumption_intensity' ) else x) # convert kwh/ft2/yr to kwh/m2/yr for the 'out.site_energy.total.energy_consumption_intensity' data column

table_com = pq.read_table('energydata/metadata-com.parquet')

df_table_com = pa.Table.to_pandas(table_com)

def df_com (feature, df_table_com, weather_file): #metrics are in consumption units

    area_sqft = feature["area"] * 10.76391  #* # area in sqft like in input data
    area_m2 = feature["area"] 

    deltaGifa = area_sqft /3  # +- 33% of the initial area, otherwise won't get enough results 

    df_weather = df_table_com[(df_table_com['in.weather_file_2018'] == weather_file)]

    #considering the whole area of the building (i.e. floor area * number of floors)
    df_area = df_weather[(df_weather['in.sqft']  < area_sqft + deltaGifa) & (df_weather['in.sqft']  > area_sqft - deltaGifa)]

    total_intensity = 0
    total_elec = 0
    total_naturalgas = 0
    elec_cooling_energy = 0
    elec_heating_energy = 0
    int_light_elec= 0
    heating_ngas = 0
    ws_ngas = 0

    if not df_area.empty:
        total_intensity = df_area['out.site_energy.total.energy_consumption'].values.mean() / area_m2 #intensity = consumption / area - kWh/m2
        total_elec = df_area['out.electricity.total.energy_consumption'].values.mean() / area_m2  
        total_naturalgas = df_area['out.natural_gas.total.energy_consumption'].values.mean() / area_m2 
        elec_cooling_energy = df_area['out.electricity.cooling.energy_consumption'].values.mean() / area_m2 
        elec_heating_energy = df_area['out.electricity.heating.energy_consumption'].values.mean() / area_m2 
        int_light_elec = df_area['out.electricity.interior_lighting.energy_consumption'].values.mean() / area_m2 
        heating_ngas = df_area['out.natural_gas.heating.energy_consumption'].values.mean() / area_m2 
        ws_ngas = df_area['out.natural_gas.water_systems.energy_consumption'].values.mean() / area_m2

    com_dict = { 'id' : feature["id"], 'name': feature.get("name"), 'housenumber': feature.get("housenumber"), 
                'street' : feature.get("street"), 'area' : area_m2, 'type' : feature["type"], 'total_intensity' : total_intensity, 'total_elec' : total_elec, 
                'total_naturalgas' : total_naturalgas , 'cooling_energy' : elec_cooling_energy, 'heating_energy' : elec_heating_energy,
                'int_light_elec': int_light_elec, 'heating_ngas': heating_ngas, 'ws_ngas': ws_ngas}

    return com_dict

def df_res(feature, df_table_m2, weather_file): #metrics are in intensity units

    #consider only floor area of buildings - i.e. total area / number of floors
    area_sqft = feature["area"] / feature["building_levels"]

    deltaGifa = area_sqft /2  # +- 50% of the initial area, otherwise won't get enough results 

    df_weather = df_table_m2[(df_table_m2['in.weather_file_2018'] == weather_file)]
 
    dfvacancy = df_weather[(df_weather['in.vacancy_status'] == 'Occupied')]

    df_area = dfvacancy[(dfvacancy['in.floor_area_conditioned_ft_2']  < area_sqft + deltaGifa) & (dfvacancy['in.floor_area_conditioned_ft_2']  > area_sqft - deltaGifa)]

    total_intensity = 0
    total_elec = 0
    total_naturalgas = 0
    elec_cooling_energy = 0
    elec_heating_energy = 0
    int_light_elec= 0
    heating_ngas = 0
    ws_ngas = 0

    if not df_area.empty:
        total_intensity =  df_area['out.site_energy.total.energy_consumption_intensity'].values.mean()
        total_elec = df_area['out.electricity.total.energy_consumption_intensity'].values.mean()
        total_naturalgas = df_area['out.natural_gas.total.energy_consumption_intensity'].values.mean()
        elec_cooling_energy = df_area['out.electricity.cooling.energy_consumption_intensity'].values.mean() * 10.76391 # convert to kWh/m2
        elec_heating_energy = df_area['out.electricity.heating.energy_consumption_intensity'].values.mean() * 10.76391
        int_light_elec = df_area['out.electricity.interior_lighting.energy_consumption_intensity'].values.mean() * 10.76391
        heating_ngas = df_area['out.natural_gas.heating.energy_consumption_intensity'].values.mean() * 10.76391
        ws_ngas = df_area['out.natural_gas.water_systems.energy_consumption_intensity'].values.mean() * 10.76391

    res_dict = { 'id' : feature["id"], 'name': feature.get("name"), 'housenumber': feature.get("housenumber"), 'street' : feature.get("street"), 
                'area' : feature["area"], 'type' : feature["type"], 'total_intensity' : total_intensity, 'total_elec' : total_elec, 'total_naturalgas' : total_naturalgas,
                'cooling_energy' : elec_cooling_energy, 'heating_energy': elec_heating_energy, 'int_light_elec': int_light_elec, 'heating_ngas': heating_ngas, 'ws_ngas': ws_ngas}

    return res_dict

# python3 program to evaluate area of a polygon using shoelace formula
# source: https://www.geeksforgeeks.org/area-of-a-polygon-with-given-n-ordered-vertices/

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

def calcAreaGeoJson (coordArray): # area in m

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

            geoJsonArea += polygonArea(xcoord, ycoord, len(xcoord))

            return geoJsonArea


#this function gets the data from the geojson file
def getIdAreaGeoJson (features): # returns array of objects with { "id": id, "name": name, "housenumber": housenumber, "street": street, "area": area, "type": type, "building_levels" : building_levels} for geoJson files from overpass API 

    geoObjArray = []

    for i in range(len(features)):
    
        id = features[i]['id']
        type = features[i]['properties'].get("building")
        name = features[i]['properties'].get("name")
        housenumber = features[i]['properties'].get("addr:housenumber")
        street = features[i]['properties'].get("addr:street")
        building_levels = float(features[i]['properties'].get("building:levels") or 1)

        # remove node elements (area = 0)
        if 'node' not in id:

            coordsArray = features[i]['geometry']['coordinates']
            area = calcAreaGeoJson(coordsArray) * building_levels

            geoObj = { "id": id, "name": name, "housenumber": housenumber, "street": street, "area": area, "type": type, "building_levels" : building_levels}
                
            geoObjArray.append(geoObj)

    return geoObjArray

# function to get data only for specific buildings in the OSM dataset (geojson input file)
def filter_by_name(building_names_array, areas_data):

    areas_data = getIdAreaGeoJson(data['features'])

    filtered_by_name = []

    for elem in areas_data:
        for building_name in building_names_array:
            if elem.get("name") == building_name:
                filtered_by_name.append(elem)

    return filtered_by_name

##########################################
 
# Opening JSON file
f = open(json_input)
  
# returns JSON object as a dictionary
data = json.load(f)

areas_data = getIdAreaGeoJson(data['features']) # floor area (1 floor)

# e.g. building names in the OSM dataset (geojson input file)
building_names = ["Coors Field","1144 Fifteenth","One Tabor Center","1801 California Street Building","Republic Plaza Building", "Colorado Convention Center", "Larimer Place Condos", "US Bank Tower", "Spire", "The Quincy", "Park Central", "17th Street Plaza", "Daniels And Fisher Tower"]

filtered_by_name = filter_by_name(building_names, areas_data) 

features(areas_data)
#features(filtered_by_name)
  
# Closing file
f.close()
