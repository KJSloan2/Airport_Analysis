import numpy as np
import csv
import math
import pandas as pd
import json

def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return str(file_encoding)

write2json = True
fName = "T_T100_SEGMENT_ALL_CARRIER_2022.csv"

selectAirport = "LAX"

df0 = pd.read_csv(str("%s%s" % (r"01_data\\",fName)),encoding="cp1252")
df0_headers = list(df0.columns.values)
dict_propKeys = {
	"_carrier":"UNIQUE_CARRIER_NAME",
	"_origin":"ORIGIN",
	"_dest":"DEST",
	"_dist_group":"DISTANCE_GROUP",
	"_class":"CLASS",
	"_dist":"DISTANCE",
	"_passengers":"PASSENGERS",
	"_freight":"FREIGHT",
	"_mail":"MAIL"
	}

class OperationalMetrics:
	def __init__(self,cdo0,cdo1,cdo2,cdo3,cdo4,cdo5,cdo6,cdo7,cdo8):
		self._carrier = cdo0
		self._origin = cdo1
		self._dest = cdo2
		self._dist = cdo3
		self._dist_group = cdo4
		self._class = cdo5
		self._passengers = cdo6
		self._freight = cdo7
		self._mail = cdo8

OpMetricsObj = OperationalMetrics([],[],[],[],[],[],[],[],[])
propKeys = []
propValSets = []

for propKey,propHeader in dict_propKeys.items():
	propValSets.append([])
	propKeys.append(propKey)
	obj_prop = (getattr(OpMetricsObj, propKey))
	if propHeader in df0_headers:
		for item in df0[propHeader]:
			obj_prop.append(item)
			if item not in propValSets[-1]:
				propValSets[-1].append(item)
if selectAirport != None:
	OpMetricsObj_subset = OperationalMetrics([],[],[],[],[],[],[],[],[])
	idx_selectOrigin = [i for i, v in enumerate(OpMetricsObj._origin) if v == selectAirport]
	idx_selectDest = [i for i, v in enumerate(OpMetricsObj._dest) if v == selectAirport]
	for idxs_ in [idx_selectOrigin,idx_selectDest]:
		for propKey in list(dict_propKeys.keys()):
			a = (getattr(OpMetricsObj, propKey))
			b = (getattr(OpMetricsObj_subset, propKey))
			for idx in idxs_:
				b.append(a[idx])
	OpMetricsObj = OpMetricsObj_subset

distRanges_ = [
	[0,499],[500,999],[1000,1499],[1500,1999],
	[2000,2499],[2500,2999],[3000,3499],
	[3500,3999],[4000,4499],[5000,5499],
	[5500,5999],[6000,6499],[6500,6999],
	[7000,7499],[7500,7999],[8000,8499],
	[8500,8999],[9000,9499],[9500,9999],
	[10000,10499],[10500,10999],[11000,11499],
	[11500,11999],[12000,"+"]
	]

def countX(lst, x):
	return lst.count(x)

def calc_stats(_data):
	stats = []
	if len(_data) >1:
		stats.append(int(sum(_data)))
		stats.append(int(np.mean(_data)))
		stats.append(int(np.std(_data)))
		stats.append(int(min(_data)))
		stats.append(int(max(_data)))
	elif len(_data) ==1:
		stats.append(int(_data[-1]))
	return stats	

path_airports = str("%s%s" % (r"00_resources\\","Airports_Global.csv"))
airports_ = pd.read_csv(path_airports,encoding='utf-8', low_memory=False)

airports_nameShort = airports_["NAME_SHORT"]
airports_country = airports_["COUNTRY_NAME"]
airports_abvs = list(airports_["LOCID_1"])
airports_lat = airports_["LAT"]
airports_lon = airports_["LON"]

dataOut_fileName = "locations.csv"
write_dataOut = open("%s%s" % (r"02_output\\",dataOut_fileName), 'w',newline='', encoding='utf-8')
writer_dataOut = csv.writer(write_dataOut)
writer_dataOut.writerow(["ID","LAT","LON","psngr_prct"])

set_links = []
#set_airports = list(dict.fromkeys(OpMetricsObj._origin+OpMetricsObj._dest))
set_airports = [selectAirport]
apDict = {}
for apKey in set_airports:
	lat,lon = (None,None)
	if apKey in airports_abvs:
		idx_locId = airports_abvs.index(apKey)
		lat = float(airports_lat[idx_locId])
		lon = float(airports_lon[idx_locId])
		if apKey not in set_links:
			set_links.append(apKey)
			writer_dataOut.writerow([
				apKey,lat,lon,""
			])
	apDict[apKey] = {
		"geo":{"lat":lat,"lon":lon},
		"metrics_summary":{
			"arrivals":{"passengers_total":0,"distance_groups":{},"mean_dist":[]},
			"departures":{"passengers_total":0,"distance_groups":{},"mean_dist":[]}},
		"Links":{
			"arrivals":{},
			"departures":{}
		}
	}
	#"hls":{"psng_tot":0,"psng_prct":0},
	keys_od = ["_origin","_dest"]
	for i in range(len(keys_od)):
		arrdept = ["departures","arrivals"][i]
		idx_od = [a for a, apVal in enumerate(getattr(OpMetricsObj, keys_od[i])) if apVal == apKey]
		keys_od_rev = list(reversed(keys_od))
		get_od = list(map(lambda idxOd: (getattr(OpMetricsObj,keys_od_rev[i]))[idxOd],idx_od))
		set_linkKeys = list(dict.fromkeys(get_od))
		
		for linkKey in set_linkKeys:
			if linkKey != selectAirport:
				apDict[apKey]["Links"][arrdept][linkKey] = {
					"airport_summary":{
						"passengers_total":0,"mean_dist":[],"passengers_prct":0
						},"distance_groups":{}
						}

				idx_linkKey = list(map(lambda idxLk:idx_od[idxLk],[lk for lk, lkVal in enumerate(get_od) if lkVal == linkKey]))
				get_distGroups = list(map(lambda idxLk: OpMetricsObj._dist_group[idxLk],idx_linkKey))
				set_distGroups = sorted(list(dict.fromkeys(get_distGroups)))

				for dgKey in set_distGroups:
					apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey] = {
						"distanceGroup_summary":{
							"mean_dist":0,
							"passengers_total":0,
							"passengers_prct":0
							},"carriers":{}
							}

					if dgKey not in list(apDict[apKey]["metrics_summary"][arrdept]["distance_groups"].keys()):
						apDict[apKey]["metrics_summary"][arrdept]["distance_groups"][dgKey] = {"passengers":{"count":0,"prct":0}}
					idx_dgKey = list(map(lambda idxDg: idx_linkKey[idxDg],[dg for dg, dgVal in enumerate(get_distGroups) if dgVal == dgKey]))
					get_dists = list(map(lambda idxDg: OpMetricsObj._dist[idxDg],idx_dgKey))
					mean_dist = np.mean(get_dists)
					apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["distanceGroup_summary"]["mean_dist"] = mean_dist
					apDict[apKey]["metrics_summary"][arrdept]["mean_dist"].append(mean_dist)
					apDict[apKey]["Links"][arrdept][linkKey]["airport_summary"]["mean_dist"].append(mean_dist)
					get_carriers = list(map(lambda idxDg: OpMetricsObj._carrier[idxDg],idx_dgKey))
					set_carriers = list(dict.fromkeys(get_carriers))

					for crKey in set_carriers:
						if crKey not in list(apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"].keys()):
							apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"][crKey] = {"passengers":{"count":0,"prct_ap":0,"prct_apdg":0,"prct_total":0}}
						idx_crKey = list(map(lambda idxCr: idx_dgKey[idxCr],[cr for cr, crVal in enumerate(get_carriers) if crVal == crKey]))
						get_passengers = int(sum(list(map(lambda idxCr: OpMetricsObj._passengers[idxCr],idx_crKey))))
						apDict[apKey]["metrics_summary"][arrdept]["passengers_total"]+=get_passengers
						apDict[apKey]["metrics_summary"][arrdept]["distance_groups"][dgKey]["passengers"]["count"]+=get_passengers
						apDict[apKey]["Links"][arrdept][linkKey]["airport_summary"]["passengers_total"]+=get_passengers
						apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["distanceGroup_summary"]["passengers_total"]+=get_passengers
						apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"][crKey]["passengers"]["count"]+=get_passengers

			apDict[apKey]["Links"][arrdept][linkKey]["airport_summary"]["mean_dist"] = np.mean(apDict[apKey]["Links"][arrdept][linkKey]["airport_summary"]["mean_dist"])
		metricKey = "passengers"
		total = int(apDict[apKey]["metrics_summary"][arrdept]["passengers_total"])
		apDict[apKey]["metrics_summary"][arrdept]["mean_dist"] = np.mean(apDict[apKey]["metrics_summary"][arrdept]["mean_dist"])
		for dgKey in list(apDict[apKey]["metrics_summary"][arrdept]["distance_groups"].keys()):
			distGroupTotal = int(apDict[apKey]["metrics_summary"][arrdept]["distance_groups"][dgKey][metricKey]["count"])
			if distGroupTotal != 0:
				apDict[apKey]["metrics_summary"][arrdept]["distance_groups"][dgKey][metricKey]["prct"] = round((100*(distGroupTotal/total)),2)
		for linkKey in set_linkKeys:
			airportTotal = int(apDict[apKey]["Links"][arrdept][linkKey]["airport_summary"]["passengers_total"])
			if airportTotal != 0:
				apDict[apKey]["Links"][arrdept][linkKey]["airport_summary"]["passengers_prct"] = round((100*(airportTotal/total)),2)

			for dgKey in list(apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"].keys()):
				carrier_keys = apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"].keys()
				apdgTotal = int(apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["distanceGroup_summary"]["passengers_total"])
				for crKey in carrier_keys:
					carrierDistGroupTotal = int(apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"][crKey][metricKey]["count"])
					if carrierDistGroupTotal != 0:
						apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"][crKey][metricKey]["prct_ap"] = round((100*(carrierDistGroupTotal/airportTotal)),2)
						apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"][crKey][metricKey]["prct_total"] = round((100*(carrierDistGroupTotal/total)),2)
						apDict[apKey]["Links"][arrdept][linkKey]["distance_groups"][dgKey]["carriers"][crKey][metricKey]["prct_apdg"] = round((100*(carrierDistGroupTotal/apdgTotal)),2)

write_dataOut.close()
if write2json == True:
	with open("%s%s%s" % (r"02_output\\",selectAirport,"_operations.json"), "w") as outfile:
		dir_pretty = json.dumps(apDict, indent=4)
		outfile.write(dir_pretty)
print("DONE")
