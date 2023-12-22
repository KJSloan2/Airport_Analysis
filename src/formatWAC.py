import numpy as np
import csv
import pandas as pd

'''This script formats airport world area code (WAC) designations'''

wacRef_ = pd.read_csv(str("%s%s" % (r"00_resources\\","L_WORLD_AREA_CODES.csv")),encoding="utf-8", low_memory=False)
wacRef_code = list(wacRef_["CODE"])
wacRef_desc = list(wacRef_["DESCRIPTION"])

regionRef_ = pd.read_csv(str("%s%s" % (r"00_resources\\","country_by_region.csv")),encoding="utf-8", low_memory=False)
regionRef_country = list(regionRef_["COUNTRY"])
regionRef_region = list(regionRef_["REGION"])
regionRef_hemisphere = list(regionRef_["HEMISPHERE"])

with open("%s%s" % (r"00_resources\\","WAC_REGIONS.csv"),'w',newline='', encoding='utf-8') as write_dataOut:
	writer_dataOut = csv.writer(write_dataOut)
	writer_dataOut.writerow(["WAC_CODE","WAC_DESC","REGION","HEMISPHERE"])
	for wacCode,wacDesc in zip(wacRef_code,wacRef_desc):
		if wacDesc in regionRef_country:
			idx_countryName = regionRef_country.index(wacDesc)
			wacRegion = regionRef_region[idx_countryName]
			wacHemisphere = regionRef_hemisphere[idx_countryName]
			writer_dataOut.writerow([wacCode,wacDesc,wacRegion,wacHemisphere])
		else:
			print(wacDesc)
write_dataOut.close()
