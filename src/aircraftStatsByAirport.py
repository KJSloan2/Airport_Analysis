#Import json for storing and writing airport stats
import json
#Import pandas for handeling dataframes
import pandas as pd
#Data source: https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMG&QO_fu146_anzr=Nv4%20Pn44vr45
######################################################################################
'''function for checking the encoding of data
the proper encoding type will be returned eg utf-8 or cp1252'''
def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return file_encoding
######################################################################################
#read reference tables for unique carrier designations
path_ucRef = str("%s%s" % (r"airports/00_resources/","L_UNIQUE_CARRIERS.csv"))
ucRef_ = pd.read_csv(path_ucRef,encoding="cp1252")
ucRef_uniqueCarrier = list(ucRef_["Code"])
ucRef_uniqueCarrierName = list(ucRef_["Description"])
######################################################################################
#read reference tables for airport type designations
path_aptRef = str("%s%s" % (r"airports/00_resources/","REF_AIRPORT_TYPE.csv"))
aptRef_ = pd.read_csv(path_aptRef,encoding="cp1252")
aptRef_iataCode = list(aptRef_["IATA_CODE"])
aptRef_type = list(aptRef_["TYPE"])
selectType = "large_airport"
idx_selectType = [i for i, v in enumerate(aptRef_type) if v == selectType]
get_iataCode = list(map(lambda idx: aptRef_iataCode[idx],idx_selectType))
######################################################################################
#read reference tables for airfcrast type designations
path_actRef = str("%s%s" % (r"airports/00_resources/","L_AIRCRAFT_TYPE.csv"))
actRef_ = pd.read_csv(path_actRef,encoding="cp1252")
actRef_code = list(actRef_["CODE"])
actRef_desc = list(actRef_["DESCRIPTION"])
actRef_make = list(actRef_["MAKE"])
actRef_hasDetails = list(actRef_["HAS_DETAILS"])
actRef_seats = list(actRef_["SEATS"])
actRef_seatGroup = list(actRef_["SEAT_GROUP"])
######################################################################################
#read reference tables for global airports information
path_aptGlobalRef = str("%s%s" % (r"airports/00_resources/","Airports_Global.csv"))
airportRef_ = pd.read_csv(path_aptGlobalRef,encoding="cp1252")
airportRef_nameLong = list(airportRef_["NAME_LONG"])
airportRef_nameShort = list(airportRef_["NAME_SHORT"])
airportRef_countryName = list(airportRef_["COUNTRY_NAME"])
airportRef_locId = list(airportRef_["LOCID"])
airportRef_lat = list(airportRef_["LAT"])
airportRef_lon = list(airportRef_["LON"])
airportRef_region = list(airportRef_["REGION"])
######################################################################################
'''this ditionary will store airport operational stats for the YOY analysis'''
years = [
	"2012","2013","2014","2015",
	"2016","2017","2018","2019",
	"2020","2021","2022", "2023"
	]

operationalStats_ = {"location_metrics":{}}
for year in years:
	operationalStats_[year] = {"departures_performed":0}
#Create a dictionary to store airport stats
#This cycles over a dataset of airports are creates a dict object for each airport 
for iataCode in get_iataCode:
	if iataCode in airportRef_locId:
		idx = airportRef_locId.index(iataCode)
		operationalStats_["location_metrics"][iataCode] = {
			"airport_id":iataCode,
			"name_short":airportRef_nameShort[idx],
			"coords":[airportRef_lat[idx],airportRef_lon[idx]],
			"region":airportRef_region[idx],
			"annual_stats":{}
		}
		for year in years:
			operationalStats_["location_metrics"][iataCode]["annual_stats"][year] = {
				"departures_performed":0,"aircraft":{}
				}
			
######################################################################################
#Read each of the designated operational metrics datasets
for year in years:
	#data_year = str(((fname.split("_")[-1]).split("."))[0])
	data_year = year
	fname = "T_T100_SEGMENT_ALL_CARRIER_"+year+".csv"
	print(data_year)
	data_ = pd.read_csv(str("%s%s" % (r"airports/01_data/",fname)),encoding="utf-8", low_memory=False)
	'''data_uniqueCarrier = list(data_["UNIQUE_CARRIER"])
	data_uniqueCarrierName = list(data_["UNIQUE_CARRIER_NAME"])
	data_aircraftType = list(data_["AIRCRAFT_TYPE"])
	data_passengers = list(data_["PASSENGERS"])
	data_seats = list(data_["SEATS"])
	data_freight = list(data_["FREIGHT"])
	data_mail = list(data_["MAIL"])
	data_deptPerf = list(data_["DEPARTURES_PERFORMED"])
	data_originWac = list(data_["ORIGIN_WAC"])'''

	data_origin = list(data_["ORIGIN"])

	unique_airportIds = list(dict.fromkeys(data_origin))
	for locidKey in list(operationalStats_["location_metrics"].keys()):
		idx_locidKey = [i for i, item in enumerate(data_origin) if item == locidKey]
		if len(idx_locidKey) !=0:
			'''get_aircraftTypes = list(map(lambda idx_i: data_aircraftType[idx_i],idx_locidKey))
			get_passengers = list(map(lambda idx_i: data_passengers[idx_i],idx_locidKey))
			get_seats = list(map(lambda idx_i: data_seats[idx_i],idx_locidKey))
			get_freight = list(map(lambda idx_i: data_freight[idx_i],idx_locidKey))
			get_mail = list(map(lambda idx_i: data_mail[idx_i],idx_locidKey))
			get_deptPerf = list(map(lambda idx_i: data_deptPerf[idx_i],idx_locidKey))
			get_carrierNames = list(map(lambda idx_i: data_uniqueCarrierName[idx_i],idx_locidKey))'''
			
			get_aircraftTypes = data_["AIRCRAFT_TYPE"].iloc[idx_locidKey].tolist()
			get_passengers = data_["PASSENGERS"].iloc[idx_locidKey].tolist()
			get_seats = data_["SEATS"].iloc[idx_locidKey].tolist()
			get_freight = data_["FREIGHT"].iloc[idx_locidKey].tolist()
			get_mail = data_["MAIL"].iloc[idx_locidKey].tolist()
			get_deptPerf = data_["DEPARTURES_PERFORMED"].iloc[idx_locidKey].tolist()
			get_carrierNames = data_["UNIQUE_CARRIER_NAME"].iloc[idx_locidKey].tolist()

			unique_aircraftTypes = list(dict.fromkeys(get_aircraftTypes))
			unique_carrierNames = list(dict.fromkeys(get_carrierNames))

			 #This section aggregates passenger stats by aircraft type
			for actKey in unique_aircraftTypes:
				aircraftTypeDesc=None
				aircraftSeats=None
				aircraftSeatGroup=None
				idx_actKey = [j for j, act in enumerate(get_aircraftTypes) if act == actKey]
				passengers = sum(list(map(lambda idx_j: get_passengers[idx_j],idx_actKey)))+1
				seats = sum(list(map(lambda idx_j: get_seats[idx_j],idx_actKey)))+1
				departuresPerformed = sum(list(map(lambda idx_j: get_deptPerf[idx_j],idx_actKey)))+1
				freight = sum(list(map(lambda idx_j: get_freight[idx_j],idx_actKey)))+1
				mail = sum(list(map(lambda idx_j: get_mail[idx_j],idx_actKey)))+1
				operationalStats_[data_year]["departures_performed"]+=departuresPerformed
				operationalStats_["location_metrics"][locidKey]["annual_stats"][data_year]["departures_performed"]+=departuresPerformed

				if actKey in actRef_code:
					idx_actRefKey = actRef_code.index(actKey)

					#Check if the refrence daraset of aircraft names has the name of the given aircraft type code
					if actRef_hasDetails[idx_actRefKey] == "T":
						aircraftTypeDesc = actRef_desc[idx_actRefKey]

					aircraftSeats = actRef_seats[idx_actRefKey]
					aircraftSeatGroup = actRef_seatGroup[idx_actRefKey]

					loadFactor = passengers/seats

					operationalStats_["location_metrics"][locidKey]["annual_stats"][data_year]["aircraft"][actKey] = {
						"aircraft_type_desc":aircraftTypeDesc,
						"seats":aircraftSeats,
						"seats_group":aircraftSeatGroup,
						"carriers":unique_carrierNames,
						"departures_performed":departuresPerformed,
						"passengers_total":passengers,
						"passengers_avg":round((passengers/departuresPerformed),2),
						"load_factor":round((loadFactor),2),
						"seats_avg":round((seats/departuresPerformed),2),
						"freight_avg":round((freight/departuresPerformed),2),
						"mail_avg":round((mail/departuresPerformed),2),
						"prct_airport_departures":0
						}

			for actKey in list(operationalStats_["location_metrics"][locidKey]["annual_stats"][data_year]["aircraft"].keys()):
				aircraft_totalDepartures = operationalStats_["location_metrics"][locidKey]["annual_stats"][data_year]["aircraft"][actKey]["departures_performed"]
				airport_totalDeparturtes = operationalStats_["location_metrics"][locidKey]["annual_stats"][data_year]["departures_performed"]
				operationalStats_["location_metrics"][locidKey]["annual_stats"][data_year]["aircraft"][actKey]["prct_airport_departures"] = round(((aircraft_totalDepartures/airport_totalDeparturtes)*100),2)
				
######################################################################################
with open(str("%s%s%s%s" % (r"airports/02_output/","operationalStats_",str(years[0]+"-"+str(years[-1]),".json")), "w", encoding='utf-8') as json_output:
	json_output.write(json.dumps(operationalStats_, indent=4, ensure_ascii=False))
######################################################################################
print("DONE")
