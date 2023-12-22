import numpy as np
import csv
import math
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import json

def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return str(file_encoding)

selectAirport = "ONT"
apKeys_ = [selectAirport]
json_ = json.load(open("%s%s%s"%(r"02_output_interum\\",selectAirport,"_operations.json")))
links_ = json_[selectAirport]["Links"]
for ad in ["arrivals","departures"]:
	for k in list(links_[ad].keys()):
		if k not in apKeys_:
			apKeys_.append(k)

df_amenities = pd.read_csv("%s%s" % (r"01_data\\",selectAirport,"temp_amenities.csv"),encoding="utf-8", low_memory=False)

_airports = list(df_amenities["AIRPORT"])
_amenities = list(df_amenities["AMENITY"])
_stars = list(df_amenities["STARS"])
scraped_amenSubCats = list(df_amenities["SUBCTRG"])
scraped_amenCtgr = list(df_amenities["CTRG"])
scraped_amenTerminal = list(df_amenities["TERMINAL"])
scraped_amenConcourse = list(df_amenities["CONCOURSE"])
scraped_amenGate = list(df_amenities["GATE"])

with open("%s%s" % (r"01_data\\",selectAirport,"Amenities Categories.csv"), 'r') as amenitiesRef__:
	reader = csv.reader(amenitiesRef__, delimiter=',')
	header = next(reader)
	amenitiesRef_ = np.array(list(reader))
ref_tf = list(amenitiesRef_[:,0])
ref_amenSubCats = list(amenitiesRef_[:,1])
ref_amenCats = list(amenitiesRef_[:,2])

amenities_dict = {}
hls_amenities = {}
for apKey in apKeys_:
	if apKey in _airports:
		print(apKey)
		idx_apKey = [i for i, ap in enumerate(_airports) if ap == apKey]
		amenities_dict[apKey] = {"total_amenities":len(idx_apKey),"ctrg":{}}
		temp_amenities_ = {
			"ctrg": list(map(lambda idx: scraped_amenCtgr[idx],idx_apKey)),
			"subCtrg":list(map(lambda idx: scraped_amenSubCats[idx],idx_apKey)),
			"names":list(map(lambda idx: _amenities[idx],idx_apKey)),
			"stars":list(map(lambda idx: _stars[idx],idx_apKey))
			}
		for ctrg in list(dict.fromkeys(temp_amenities_["ctrg"])):
			idx_ctrg = [j for j, c in enumerate(temp_amenities_["ctrg"]) if c == ctrg]
			subCtrgs_ = list(map(lambda idx: temp_amenities_["subCtrg"][idx], idx_ctrg))
			ctrg_names = list(map(lambda idx: temp_amenities_["names"][idx], idx_ctrg))
			ctrg_stars = list(map(lambda idx: temp_amenities_["stars"][idx], idx_ctrg))

			if ctrg not in list(hls_amenities.keys()):
				hls_amenities[ctrg] = {"subCtrg":{}}

			for subCtrg in list(dict.fromkeys(subCtrgs_)):
				idx_ctrg = [k for k, sc in enumerate(subCtrgs_) if sc == subCtrg]
				subCtrg_names = list(map(lambda idx: ctrg_names[idx], idx_ctrg))
				if subCtrg not in list(hls_amenities[ctrg]["subCtrg"].keys()):
					hls_amenities[ctrg]["subCtrg"][subCtrg] = {"airports":{},"names":{},"stars":[]}
				for name in subCtrg_names:
					split_name = name.split("-")
					nameKeys_ = list(hls_amenities[ctrg]["subCtrg"][subCtrg]["names"].keys())
					if name not in nameKeys_:
						hls_amenities[ctrg]["subCtrg"][subCtrg]["names"][name] = {"f":1}
					elif name in nameKeys_:
						hls_amenities[ctrg]["subCtrg"][subCtrg]["names"][name]["f"]+=1
							
				subCtrg_stars = list(map(lambda idx: ctrg_stars[idx], idx_ctrg))
				count_typeKey = len(idx_ctrg)
				stars_formated = []
				reviews = []
				for v in subCtrg_stars:
					try:
						if v != "nan" and math.isnan(float(v)) == False:
							split_v = str(v).split(".")
							if split_v[1] == "0":
								stars_formated.append(int(split_v[0]))
							else:
								stars_formated.append(float(v))
					except:
						continue

				ctrg_keys = list(amenities_dict[apKey]["ctrg"].keys())
				if ctrg not in ctrg_keys:
					amenities_dict[apKey]["ctrg"][ctrg] = {"subCtrg":{}}
					amenities_dict[apKey]["ctrg"][ctrg]["subCtrg"][subCtrg] = {"prct":None,"names":subCtrg_names,"mean_stars":stars_formated}
				elif ctrg in ctrg_keys:
					if subCtrg not in list(amenities_dict[apKey]["ctrg"][ctrg]["subCtrg"].keys()):
						amenities_dict[apKey]["ctrg"][ctrg]["subCtrg"][subCtrg] = {"prct":None,"names":subCtrg_names,"mean_stars":stars_formated}
				elif ctrg in ctrg_keys and subCtrg in list(amenities_dict[apKey]["ctrg"][ctrg]["subCtrg"].keys()):
					subCtrg_ = amenities_dict[apKey]["ctrg"][ctrg]["subCtrg"][subCtrg]
					for n,s in zip(subCtrg_names,subCtrg_stars):
						subCtrg_["names"].append(n)
						subCtrg_["mean_stars"].append(s)
					
				subCrtg_ = amenities_dict[apKey]["ctrg"][ctrg]["subCtrg"][subCtrg]
				if len(subCrtg_["mean_stars"]) != 0:
					subCrtg_["mean_stars"] = round((np.mean(subCrtg_["mean_stars"])),2)
				subCrtg_["prct"] = round((100*(len(subCrtg_["names"])/amenities_dict[apKey]["total_amenities"])),2)

for ctrg in list(hls_amenities.keys()):
	for subCtrg in list(hls_amenities[ctrg]["subCtrg"].keys()):
		for name in list(hls_amenities[ctrg]["subCtrg"][subCtrg]["names"].keys()):
			f = hls_amenities[ctrg]["subCtrg"][subCtrg]["names"][name]["f"]
			print(ctrg,subCtrg,name,f)

write2json = True
if write2json == True:
	with open("%s%s%s" % (r"02_output_interum\\",selectAirport,"_amenitiesAggregated.json"), "w") as outfile:
		dir_pretty = json.dumps(amenities_dict, indent=4)
		outfile.write(dir_pretty)

print("DONE")
