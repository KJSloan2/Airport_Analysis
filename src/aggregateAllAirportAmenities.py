import numpy as np
import csv
import math
import pandas as pd
import time
import os
from os import listdir
from os.path import isfile, join
import json

tempAmenities_fileName = "temp_amenities.csv"
df_amenities = pd.read_csv(str("%s%s" % (r"01_data\\",tempAmenities_fileName)),encoding="utf-8")

_airports = list(df_amenities["AIRPORT"])
_amenities = list(df_amenities["AMENITY"])
_stars = list(df_amenities["STARS"])
_reviewCounts = list(df_amenities["REVIEWS"])
_types = list(df_amenities["TYPE"])

set_airports = list(dict.fromkeys(_airports))
amenitiesRef_filePath = "%s%s" % (r"00_resources\\","Amenities Categories.csv")
with open(amenitiesRef_filePath, 'r') as amenitiesRef__:
	reader = csv.reader(amenitiesRef__, delimiter=',')
	header = next(reader)
	amenitiesRef_ = np.array(list(reader))
amenitiesRef_types = list(amenitiesRef_[:,0])
amenitiesRef_tf = list(amenitiesRef_[:,0])
amenitiesRef_type1 = list(amenitiesRef_[:,1])
amenitiesRef_type0 = list(amenitiesRef_[:,2])

amenities_dict = {}
amenitiesStats_dict = {}
for apKey in set_airports:
	idx_apKey = [i for i, apVal in enumerate(_airports) if apVal == apKey]
	amenities_dict[apKey] = {"total_amenities":len(idx_apKey),"amenities_by_type":{}}
	get_apAmenityTypes = list(map(lambda idx: _types[idx], idx_apKey))
	get_apAmenityNames = list(map(lambda idx: _amenities[idx], idx_apKey))
	set_apAmenityTypes = list(dict.fromkeys(get_apAmenityTypes))
	for tier1 in set_apAmenityTypes:
		if tier1 in amenitiesRef_type1:
			idxTier1 = amenitiesRef_type1.index(tier1)
			if amenitiesRef_tf[idxTier1] == "T":
				tier0 = amenitiesRef_type0[idxTier1]
				idx_typeKey = [j for j, tkVal in enumerate(get_apAmenityTypes) if tkVal == tier1]
				get_apAmenityNames_byType = list(map(lambda idxTk: get_apAmenityNames[idxTk], idx_typeKey))
				count_typeKey = len(idx_typeKey)
				idx_subset = list(map(lambda idxTK: idx_apKey[idxTK],idx_typeKey))
				stars_ = []
				reviews = []
				for idx in idx_subset:
					v = _stars[idx]
					try:
						if v != "nan" and math.isnan(float(v)) == False:
							split_v = str(v).split(".")
							if split_v[1] == "0":
								stars_.append(int(split_v[0]))
							else:
								stars_.append(float(v))
					except:
						continue	

				percent_typeKey = round((100*(count_typeKey/amenities_dict[apKey]["total_amenities"])),2)
				meanStars = None
				if len(stars_) != 0:
					meanStars = round((np.mean(stars_)),2)
				if tier0 not in list(amenitiesStats_dict.keys()):
					amenitiesStats_dict[tier0] = {tier1:{"airports":[apKey],"stars":[meanStars]}}
				elif tier0 in list(amenitiesStats_dict.keys()):
					if tier1 not in list(amenitiesStats_dict[tier0].keys()):
						amenitiesStats_dict[tier0][tier1] = {"airports":[apKey],"stars":[meanStars]}
				elif tier0 in list(amenitiesStats_dict.keys()) and tier1 in list(amenitiesStats_dict[tier0].keys()):
					if apKey not in amenitiesStats_dict[tier0][tier1]["airports"]:
						amenitiesStats_dict[tier0][tier1]["airports"].append(apKey)
						amenitiesStats_dict[tier0][tier1]["airports"].append(meanStars)
				amenities_dict[apKey]["amenities_by_type"][tier0] = {
					"amen_type_tier_1":tier1,
					"count":count_typeKey,
					"percent_of_amenities":percent_typeKey,
					"amenities":get_apAmenityNames_byType,
					"review_stats":{
						"review_count":"",
						"mean_stars": meanStars
						}
						}

write2json = True
if write2json == True:
	with open("%s%s" % (r"01_data\\","amenitiesAggregated.json"), "w") as outfile:
		dir_pretty = json.dumps(amenities_dict, indent=4)
		outfile.write(dir_pretty)

print("DONE")
