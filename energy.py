import socketio
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

sio = socketio.Server(cors_allowed_origins="*")

app = socketio.WSGIApp(sio)

@sio.event
def connect(sid, environ):
    print(sid, 'connected')
   
@sio.event
def disconnect(sid):
    print(sid, 'disconnected')

@sio.event
def ifcAreas(sid, data): # use the ifc data as input to the energy functions
    #print(sid, data)

    windowArea = data['windowArea']
    wallArea = data['wallArea']

    # workaround divide by zero
    if (windowArea == 0):
        windowArea = 1
    if (wallArea == 0):
        wallArea = 1

    wwr = windowArea / wallArea
    slabAreasqft = data['slabArea'] * 10.76391 # convert from m2 (IFC) to ft2 (parquet data)

    df_usage = df_initial(slabAreasqft,df_table_m2)
    df_bench = df_benchmark(df_usage)

    df_ori = df_generic(df_usage, ['East','Northeast','North','Northwest','West','Southwest','South','Southeast','BIM'], 'in.orientation')
    df_wwr = df_wwratio(df_usage, wwr)
    df_wall = df_generic(df_usage, ['Brick, 12-in, 3-wythe, R-11','Brick, 12-in, 3-wythe, R-15','Brick, 12-in, 3-wythe, R-19','Brick, 12-in, 3-wythe, R-7','Brick, 12-in, 3-wythe, Uninsulated','CMU, 6-in Hollow, R-11','CMU, 6-in Hollow, R-15','CMU, 6-in Hollow, R-19','CMU, 6-in Hollow, R-7','CMU, 6-in Hollow, Uninsulated','Wood Stud, R-11','Wood Stud, R-15','Wood Stud, R-19','Wood Stud, R-7','Wood Stud, Uninsulated', 'BIM'], 'in.insulation_wall')
    df_window = df_generic(df_usage, ['Double, Clear, Metal, Air','Double, Clear, Metal, Air, Exterior Clear Storm','Double, Clear, Non-metal, Air','Double, Clear, Non-metal, Air, Exterior Clear Storm','Double, Low-E, Non-metal, Air, M-Gain','Single, Clear, Metal','Single, Clear, Metal, Exterior Clear Storm','Single, Clear, Non-metal,','Single, Clear, Non-metal, Exterior Clear Storm','Triple, Low-E, Non-metal, Air, L-Gain','BIM'], 'in.windows')
    df_roof = df_generic(df_usage, ['Finished, R-13','Finished, R-19','Finished, R-30','Finished, R-38','Finished, R-49','Finished, R-7','Finished, Uninsulated','Unfinished, Uninsulated', 'BIM'], 'in.insulation_roof')
    df_inf = df_generic(df_usage, ['1 ACH50','10 ACH50','15 ACH50','2 ACH50','20 ACH50','25 ACH50','3 ACH50','30 ACH50','4 ACH50','40 ACH50','5 ACH50','50 ACH50','6 ACH50','7 ACH50','8 ACH50','BIM'], 'in.infiltration')
    df_plug = df_generic(df_usage, ['50%', '100%', '200%', 'BIM'], 'in.plug_load_diversity')
    df_hvac_h = df_generic(df_usage, ['Ducted Heat Pump','Ducted Heating','Non-Ducted Heating','None', 'BIM'] ,'in.hvac_heating_type')
    df_hvac_c = df_generic(df_usage, ['Central AC','Heat Pump','None','Room AC', 'BIM'] , 'in.hvac_cooling_type' )
    df_pv = df_generic (df_usage,  ['1.0 kWDC','11.0 kWDC','13.0 kWDC','3.0 kWDC','5.0 kWDC','7.0 kWDC','9.0 kWDC','None', 'BIM'], 'in.pv_system_size')

    #emit the data for each graph
    sio.emit('df_benchmark', df_bench)
    sio.emit('df_orientation', df_ori)
    sio.emit('df_wwr', df_wwr)
    sio.emit('df_wall', df_wall)
    sio.emit('df_roof', df_roof)
    sio.emit('df_window', df_window)
    sio.emit('df_inf', df_inf)
    sio.emit('df_plug', df_plug)
    sio.emit('df_hvac_h', df_hvac_h)
    sio.emit('df_hvac_c', df_hvac_c)
    sio.emit('df_pv', df_pv)

    # emit initial options to filter
    sio.emit('occupancy_values', occupancy_filter())
    sio.emit('usage_values', usage_filter())
    sio.emit('vintage_values', vintage_filter())

@sio.event
def updateFilter(sid, data):
    print(sid, data)

    windowArea = data[0]['windowArea']
    wallArea = data[0]['wallArea']

   # workaround divide by zero
    if (windowArea == 0):
        windowArea = 1

    if (wallArea == 0):
        wallArea = 1

    wwr = windowArea / wallArea
    slabAreasqft = data[0]['slabArea'] * 10.76391 # convert from m2 (IFC) to ft2 (parquet data)

    df_usage = df_uifilter(slabAreasqft, data, df_table_m2)
    df_bench = df_benchmark(df_usage) 

    # inputs from the parquet file - see data dictionary file for more info: https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2021/resstock_amy2018_release_1/data_dictionary.tsv
    df_ori = df_generic(df_usage, ['East','Northeast','North','Northwest','West','Southwest','South','Southeast','BIM'], 'in.orientation')
    df_wwr = df_wwratio(df_usage, wwr)
    df_wall = df_generic(df_usage, ['Brick, 12-in, 3-wythe, R-11','Brick, 12-in, 3-wythe, R-15','Brick, 12-in, 3-wythe, R-19','Brick, 12-in, 3-wythe, R-7','Brick, 12-in, 3-wythe, Uninsulated','CMU, 6-in Hollow, R-11','CMU, 6-in Hollow, R-15','CMU, 6-in Hollow, R-19','CMU, 6-in Hollow, R-7','CMU, 6-in Hollow, Uninsulated','Wood Stud, R-11','Wood Stud, R-15','Wood Stud, R-19','Wood Stud, R-7','Wood Stud, Uninsulated', 'BIM'], 'in.insulation_wall')
    df_window = df_generic(df_usage, ['Double, Clear, Metal, Air','Double, Clear, Metal, Air, Exterior Clear Storm','Double, Clear, Non-metal, Air','Double, Clear, Non-metal, Air, Exterior Clear Storm','Double, Low-E, Non-metal, Air, M-Gain','Single, Clear, Metal','Single, Clear, Metal, Exterior Clear Storm','Single, Clear, Non-metal,','Single, Clear, Non-metal, Exterior Clear Storm','Triple, Low-E, Non-metal, Air, L-Gain','BIM'], 'in.windows')
    df_roof = df_generic(df_usage, ['Finished, R-13','Finished, R-19','Finished, R-30','Finished, R-38','Finished, R-49','Finished, R-7','Finished, Uninsulated','Unfinished, Uninsulated', 'BIM'], 'in.insulation_roof')
    df_inf = df_generic(df_usage, ['1 ACH50','10 ACH50','15 ACH50','2 ACH50','20 ACH50','25 ACH50','3 ACH50','30 ACH50','4 ACH50','40 ACH50','5 ACH50','50 ACH50','6 ACH50','7 ACH50','8 ACH50','BIM'], 'in.infiltration')
    df_plug = df_generic(df_usage, ['50%', '100%', '200%', 'BIM'], 'in.plug_load_diversity')
    df_hvac_h = df_generic(df_usage, ['Ducted Heat Pump','Ducted Heating','Non-Ducted Heating','None', 'BIM'] ,'in.hvac_heating_type')
    df_hvac_c = df_generic(df_usage, ['Central AC','Heat Pump','None','Room AC', 'BIM'] , 'in.hvac_cooling_type' )
    df_pv = df_generic (df_usage,  ['1.0 kWDC','11.0 kWDC','13.0 kWDC','3.0 kWDC','5.0 kWDC','7.0 kWDC','9.0 kWDC','None', 'BIM'], 'in.pv_system_size')

    sio.emit('df_benchmark', df_bench) 
    sio.emit('df_orientation', df_ori)
    sio.emit('df_wwr', df_wwr)
    sio.emit('df_wall', df_wall)
    sio.emit('df_roof', df_roof)
    sio.emit('df_window', df_window)
    sio.emit('df_inf', df_inf)
    sio.emit('df_plug', df_plug)
    sio.emit('df_hvac_h', df_hvac_h)
    sio.emit('df_hvac_c', df_hvac_c)
    sio.emit('df_pv', df_pv)

    print(windowArea)

table = pq.read_table('energydata/metadata.parquet') 

df_table = pa.Table.to_pandas(table) 

df_table_m2 = df_table.apply(lambda x : x * 10.76391 if x.name == 'out.site_energy.total.energy_consumption_intensity' else x) # convert kwh/ft2/yr to kwh/m2/yr for the 'out.site_energy.total.energy_consumption_intensity' data column

df_avg_ei = df_table['out.site_energy.total.energy_consumption_intensity'].mean()

def occupancy_filter():
    occupancy_values = df_table['in.occupants'].unique()
    occupancy_values_sorted = np.sort(occupancy_values).tolist()
    return occupancy_values_sorted

def usage_filter():
    usage_values = df_table['in.usage_level'].unique()
    usage_values_sorted = np.sort(usage_values).tolist()
    return usage_values_sorted

def vintage_filter():
    vintage_values = df_table['in.vintage'].unique()
    vintage_values_sorted = np.sort(vintage_values).tolist()
    return vintage_values_sorted

def df_uifilter (slabArea, uiparams, df_table_m2):
    deltaarea = slabArea /10 # +- 10% of the initial area

    dfvacancy = df_table_m2[(df_table_m2['in.vacancy_status'] == 'Occupied')]

    df_area = dfvacancy[(dfvacancy['in.floor_area_conditioned_ft_2']  < slabArea + deltaarea) & (dfvacancy['in.floor_area_conditioned_ft_2']  > slabArea - deltaarea)]

    if ((uiparams[1]['occupancy'] == ['Occupancy']) | (uiparams[1]['occupancy'] == ['All'])):
        dfoccupancy = df_area
    else:
        dfoccupancy = df_area[(df_area['in.occupants'].isin(uiparams[1]['occupancy']))]

    # dfusage

    if ((uiparams[1]['usage'] == ['Usage']) | (uiparams[1]['usage'] == ['All'])):
        df_usage = dfoccupancy
    else:
        df_usage = dfoccupancy[(dfoccupancy['in.usage_level'].isin(uiparams[1]['usage']))]

    # dfvintage
    if ((uiparams[1]['vintage'] == ['Vintage']) | (uiparams[1]['vintage'] == ['All'])):
        df_vintage = df_usage
    else:
        df_vintage = df_usage[(df_usage['in.vintage'].isin(uiparams[1]['vintage']))]

    return df_vintage

def df_initial(slabArea, df_table_m2): 

    deltaarea = slabArea /10 # +- 10% of the initial area

    dfvacancy = df_table_m2[(df_table_m2['in.vacancy_status'] == 'Occupied')] 
    df_area = dfvacancy[(dfvacancy['in.floor_area_conditioned_ft_2']  < slabArea + deltaarea) & (dfvacancy['in.floor_area_conditioned_ft_2']  > slabArea - deltaarea)] # ifc floor area

    return df_area

def df_benchmark(data_table): 
    category = ['BIM']

    values_m2 = data_table['out.site_energy.total.energy_consumption_intensity'].values 

    data_benchmark = [values_m2.tolist()]

    return category, data_benchmark

def df_wwratio(df_table, wwr_bim): 
    wwr_values = ['F6 B6 L6 R6','F9 B9 L9 R9','F15 B15 L15 R15','F18 B18 L18 R18','F30 B30 L30 R30', 'BIM']

    # which interval is wwr_bim ei in
    if (wwr_bim < 0.09 ):
        df_wwr_bim_cat = wwr_values[0]
    elif (0.09 < wwr_bim < 0.15 ):
        df_wwr_bim_cat = wwr_values[1]
    elif (0.15 < wwr_bim < 0.18):
        df_wwr_bim_cat = wwr_values[2]
    elif (0.18 < wwr_bim < 0.30):
        df_wwr_bim_cat = wwr_values[3]
    else:
        df_wwr_bim_cat = wwr_values[4]

    df_wwr_bim = df_table [df_table['in.window_areas'] == df_wwr_bim_cat]

    df_wwr_bim_ei = df_wwr_bim['out.site_energy.total.energy_consumption_intensity'].mean()

    wwr_labels_values = []
    df_wwr_array = []
    df_wwr_array_json = []

    for i in range(len(wwr_values)):
        df_wwr = df_table [df_table['in.window_areas'] == wwr_values[i]]

        if not df_wwr.empty:
            wwr_labels_values.append(wwr_values[i])
            df_wwr_array.append(df_wwr['out.site_energy.total.energy_consumption_intensity'] )

    for i in range(len(df_wwr_array)):
        df_wwr_array_json.append(df_wwr_array[i].values.tolist())
    
    df_wwr_array_json.append([df_table['out.site_energy.total.energy_consumption_intensity'].median() ])
    #append BIM category / values at the end of the arrays
    wwr_labels_values.append(wwr_values[len(wwr_values) -1])

    return wwr_labels_values, df_wwr_array_json

def df_generic (data_table, x_values, filter_param):

    or_labels_values = [] 
    df_or_array = []
    df_or_array_json = []

    for i in range(len(x_values)):
        df_or = data_table [data_table[filter_param] == x_values[i]]

        if not df_or.empty:
            or_labels_values.append(x_values[i])
            df_or_array.append(df_or['out.site_energy.total.energy_consumption_intensity'] )

    or_labels_values.append(x_values[len(x_values) -1]) #need to add BIM category at the end

    for i in range(len(df_or_array)):
        df_or_array_json.append(df_or_array[i].values.tolist())

    df_or_array_json.append([data_table['out.site_energy.total.energy_consumption_intensity'].median() ])

    return or_labels_values, df_or_array_json    