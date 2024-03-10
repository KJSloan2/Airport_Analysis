#Import json for storing and writing airport stats
import json
#Import pandas for handeling dataframes
import pandas as pd

######################################################################################
'''function for checking the encoding of data
the proper encoding type will be returned eg utf-8 or cp1252'''
def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return str(file_encoding)
######################################################################################
#read reference tables for unique carrier designations
path_ucRef = str("%s%s" % (r"airports/00_resources/","L_UNIQUE_CARRIERS.csv"))
ucRef_ = pd.read_csv(path_ucRef,encoding=check_encoding(path_ucRef))
ucRef_uniqueCarrier = list(ucRef_["Code"])
ucRef_uniqueCarrierName = list(ucRef_["Description"])
######################################################################################
#read reference tables for airport type designations
path_aptRef = str("%s%s" % (r"airports/00_resources/","REF_AIRPORT_TYPE.csv"))
aptRef_ = pd.read_csv(path_aptRef,check_encoding(path_aptRef))
aptRef_iataCode = list(aptRef_["IATA_CODE"])
aptRef_type = list(aptRef_["TYPE"])
selectType = "large_airport"
idx_selectType = [i for i, v in enumerate(aptRef_type) if v == selectType]
get_iataCode = list(map(lambda idx: aptRef_iataCode[idx],idx_selectType))
######################################################################################
#read reference tables for airfcrast type designations
path_actRef = str("%s%s" % (r"airports/00_resources/","L_AIRCRAFT_TYPE.csv"))
actRef_ = pd.read_csv(path_actRef,check_encoding(path_actRef))
actRef_code = list(actRef_["CODE"])
actRef_desc = list(actRef_["DESCRIPTION"])
actRef_make = list(actRef_["MAKE"])
actRef_hasDetails = list(actRef_["HAS_DETAILS"])
actRef_seats = list(actRef_["SEATS"])
actRef_seatGroup = list(actRef_["SEAT_GROUP"])
######################################################################################
#read reference tables for global airports information
path_aptGlobalRef = str("%s%s" % (r"airports/00_resources/","Airports_Global.csv"))
airportRef_ = pd.read_csv(path_aptGlobalRef,encoding=check_encoding(path_aptGlobalRef))
airportRef_nameLong = list(airportRef_["NAME_LONG"])
airportRef_nameShort = list(airportRef_["NAME_SHORT"])
airportRef_countryName = list(airportRef_["COUNTRY_NAME"])
airportRef_locId = list(airportRef_["LOCID"])
airportRef_lat = list(airportRef_["LAT"])
airportRef_lon = list(airportRef_["LON"])
airportRef_region = list(airportRef_["REGION"])
######################################################################################
#set the names of the datasets to analyize (must be T100 segment data)
files_ = ["T_T100_SEGMENT_ALL_CARRIER_2012.csv","T_T100_SEGMENT_ALL_CARRIER_2022.csv"]

'''this ditionary will store airport operational stats for the YOY analysis'''
operationalStats_ = {
	"2012":{"departures_performed":0},
	"2022":{"departures_performed":0},
	"location_metrics":{}
	}
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
			"2012":{"departures_performed":0,"aircraft":{}},
			"2022":{"departures_performed":0,"aircraft":{}}
		}
######################################################################################
#Read each of the designated operational metrics datasets
for fname in files_:
	data_year = str(((fname.split("_")[-1]).split("."))[0])
	print(data_year)
	data_ = pd.read_csv(str("%s%s" % (r"airports/01_data/",fname)),encoding="utf-8", low_memory=False)
	data_uniqueCarrier = list(data_["UNIQUE_CARRIER"])
	data_uniqueCarrierName = list(data_["UNIQUE_CARRIER_NAME"])
	data_aircraftType = list(data_["AIRCRAFT_TYPE"])
	data_origin = list(data_["ORIGIN"])
	data_passengers = list(data_["PASSENGERS"])
	data_seats = list(data_["SEATS"])
	data_freight = list(data_["FREIGHT"])
	data_mail = list(data_["MAIL"])
	data_deptPerf = list(data_["DEPARTURES_PERFORMED"])
	data_originWac = list(data_["ORIGIN_WAC"])

	aggregator = "ORIGIN"
	#departuresPerformed_annualTotal = sum()
	sansDup_airportIds = list(dict.fromkeys(data_origin))
	for locidKey in list(operationalStats_["location_metrics"].keys()):
		idx_locidKey = [i for i, item in enumerate(data_origin) if item == locidKey]
		if len(idx_locidKey) !=0:
			get_aircraftTypes = list(map(lambda idx_i: data_aircraftType[idx_i],idx_locidKey))
			get_passengers = list(map(lambda idx_i: data_passengers[idx_i],idx_locidKey))
			get_seats = list(map(lambda idx_i: data_seats[idx_i],idx_locidKey))
			get_freight = list(map(lambda idx_i: data_freight[idx_i],idx_locidKey))
			get_mail = list(map(lambda idx_i: data_mail[idx_i],idx_locidKey))
			get_deptPerf = list(map(lambda idx_i: data_deptPerf[idx_i],idx_locidKey))
			get_carrierNames = list(map(lambda idx_i: data_uniqueCarrierName[idx_i],idx_locidKey))

			sansDup_aircraftTypes = list(dict.fromkeys(get_aircraftTypes))
			sansDup_carrierNames = list(dict.fromkeys(get_carrierNames))
			for actKey in sansDup_aircraftTypes:
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
				operationalStats_["location_metrics"][locidKey][data_year]["departures_performed"]+=departuresPerformed

				if actKey in actRef_code:
					idx_actRefKey = actRef_code.index(actKey)
					if actRef_hasDetails[idx_actRefKey] == "T":
						aircraftTypeDesc = actRef_desc[idx_actRefKey]
						aircraftSeats = actRef_seats[idx_actRefKey]
						aircraftSeatGroup = actRef_seatGroup[idx_actRefKey]

						loadFactor = passengers/seats

						operationalStats_["location_metrics"][locidKey][data_year]["aircraft"][actKey] = {
							"aircraft_type_desc":aircraftTypeDesc,
							"seats":aircraftSeats,
							"seats_group":aircraftSeatGroup,
							"carriers":sansDup_carrierNames,
							"departures_performed":departuresPerformed,
							"passengers_total":passengers,
							"passengers_avg":round((passengers/departuresPerformed),2),
							"load_factor":round((loadFactor),2),
							"seats_avg":round((seats/departuresPerformed),2),
							"freight_avg":round((freight/departuresPerformed),2),
							"mail_avg":round((mail/departuresPerformed),2),
							"prct_airport_departures":0
							}

			for actKey in list(operationalStats_["location_metrics"][locidKey][data_year]["aircraft"].keys()):
				operationalStats_["location_metrics"][locidKey][data_year]["aircraft"][actKey]["prct_airport_departures"] = round(
					((operationalStats_["location_metrics"][locidKey][data_year]["aircraft"][actKey]["departures_performed"]/operationalStats_["location_metrics"][locidKey][data_year]["departures_performed"])*100)
					,2)
				
######################################################################################
with open(str("%s%s%s%s" % (r"airports/02_output/","origin_fleet_","2012-2022",".json")), "w", encoding='utf-8') as json_output:
	json_output.write(json.dumps(operationalStats_, indent=4, ensure_ascii=False))
######################################################################################
print("DONE")
