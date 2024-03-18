import json
import csv
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
######################################################################################
def insertion_sort(arr):
	arr = list(map(int,arr))
	print(arr)
	for i in range(1, len(arr)):
		key = arr[i]
		j = i - 1
		while j >= 0 and arr[j] > key:
			arr[j + 1] = arr[j]
			j -= 1
		arr[j + 1] = key
######################################################################################
def insertion_sort_years(arr):
	arr = list(map(int,arr))
	for i in range(1, len(arr)):
		key = arr[i]
		j = i - 1
		while j >= 0 and arr[j] > key:
			arr[j + 1] = arr[j]
			j -= 1
		arr[j + 1] = key
######################################################################################
def simple_linear_regression(x, y,x_type,y_type, predictionYears):
	#convert data types of x and y based on user input
	if x_type == "int":
		x = list(map(int,x))
	elif x_type == "float":
		x = list(map(float,x))

	if y_type == "int":
		y = list(map(int,y))
	elif y_type == "float":
		y = list(map(float,y))

	n = len(x)

	# Calculate the mean of x and y
	mean_x = sum(x) / n
	mean_y = sum(y) / n

	# Calculate the slope (m) of the regression line
	numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
	denominator = sum((x[i] - mean_x) ** 2 for i in range(n))
	m = numerator / denominator

	# Calculate the intercept (b) of the regression line
	b = mean_y - m * mean_x

	predicted_values = []
	for year in range(predictionYears[0], predictionYears[-1]):
		predicted_value = m * year + b
		predicted_values.append(predicted_value)

	return m, b, predicted_values
######################################################################################
#####################################################################################
def normailize_list(vals_):
	vals_min = min(vals_)
	vals_max = max(vals_)
	vals_norm = []
	for val in vals_:
		vals_norm.append((val-vals_min)/(vals_max-vals_min))
	return vals_norm
#####################################################################################
def normailize_val(val,d_min,d_max):
	return round(((val-d_min)/(d_max-d_min)),4)
#####################################################################################
def hypothesis(theta, X):
    return theta*X

def computeCost(X, y, theta):
    y1 = hypothesis(theta, X)
    y1=np.sum(y1, axis=1)
    return sum(np.sqrt((y1-y)**2))/(2*47)

def gradientDescent(X, Y, theta, alpha, i):
    J = []  #cost function in each iterations
    k = 0
    while k < i:        
        y1 = hypothesis(theta, X)
        y1 = np.sum(y1, axis=1)
        for c in range(0, len(X.columns)):
            theta[c] = theta[c] - alpha*(sum((y1-y)*X.iloc[:,c])/len(X))
        j = computeCost(X, Y, theta)
        J.append(j)
        k += 1
    return J, j, theta
######################################################################################
######################################################################################
data = json.load(open(r"airports/02_output/operationalStats_2012-2023.json"))
######################################################################################
data_aircraftStats = {
	"YEAR":[],
	"AIRCRAFT_TYPE":[],
	"PRCT_AIRPORT_DEPARTURES":[]
}
ref_aircraftTypes = {}
######################################################################################
for airportKey, airportStats in data["location_metrics"].items():
	for yearKey, yearStats in airportStats["annual_stats"].items():
		for aircraftKey,aircraftStats in yearStats["aircraft"].items():
			data_aircraftStats["YEAR"].append(yearKey)
			data_aircraftStats["AIRCRAFT_TYPE"].append(aircraftKey)
			data_aircraftStats["PRCT_AIRPORT_DEPARTURES"].append(aircraftStats["prct_airport_departures"])

			if aircraftKey not in list(ref_aircraftTypes.keys()):
				ref_aircraftTypes[aircraftKey] = {"TYPE_DESC":aircraftStats["aircraft_type_desc"]}
			
######################################################################################
df_aircraftStats = pd.DataFrame(data_aircraftStats)
unique_aircraftTypeKeys = list(dict.fromkeys(data_aircraftStats["AIRCRAFT_TYPE"]))
unique_yearKeys = list(dict.fromkeys(data_aircraftStats["YEAR"]))
insertion_sort_years(unique_yearKeys)
######################################################################################
fig = go.Figure()
model_linReg = LinearRegression()
######################################################################################
with open("%s%s" % (r"airports/02_output/","aircraftType_annualStats.csv"),'w',newline='', encoding='utf-8') as write_dataOut:
	writer_dataOut = csv.writer(write_dataOut)
	writer_dataOut.writerow([
		"YEAR","AIRCRAFT_TYPE_KEY","AIRCRAFT_TYPE_DESC",
		"MEAN_PRCT_TOT_AIRPORT_DEPARTURES","STD_PRCT_TOT_AIRPORT_DEPARTURES",
		"MAX_PRCT_TOT_AIRPORT_DEPARTURES"
		])
	
	for aircraftTypeKey in unique_aircraftTypeKeys:
		#Get the na me/descriuption of the aircraft type 
		aircraftTypeDesc = ref_aircraftTypes[aircraftTypeKey]["TYPE_DESC"]
		if aircraftTypeDesc != None:
			#Get the row indexes of the aircraftTypeKey in the dataframe
			idx_aircraftTypeKey = [i for i, val in enumerate(data_aircraftStats["AIRCRAFT_TYPE"]) if val == aircraftTypeKey]
			#Get data from the indexed rows
			act_years = df_aircraftStats["YEAR"].iloc[idx_aircraftTypeKey].tolist()
			act_prctAirportDepartures = df_aircraftStats["PRCT_AIRPORT_DEPARTURES"].iloc[idx_aircraftTypeKey].tolist()
			#Summarize the aircraft type's operational metrrics by year
			X, Y = [],[]
			try:
				for yearKey in unique_yearKeys:
					if yearKey in act_years:
						year_prctAirportDepartures = df_aircraftStats.loc[df_aircraftStats['YEAR'] == yearKey, 'PRCT_AIRPORT_DEPARTURES']
						mean_prctAirportDepartures = np.mean(year_prctAirportDepartures)
						std_prctAirportDepartures = np.std(year_prctAirportDepartures)
						max_prctAirportDepartures = max(year_prctAirportDepartures)

						X.append([int(yearKey)])
						Y.append(mean_prctAirportDepartures)
					
						writer_dataOut.writerow([
							yearKey,
							aircraftTypeKey,
							aircraftTypeDesc,
							mean_prctAirportDepartures,
							std_prctAirportDepartures,
							max_prctAirportDepartures
							])
						
				'''theta = np.array([0]*len(X))
				J, j, theta = gradientDescent(X, Y, theta, 0.05, 10000)

				y_hat = hypothesis(theta, X)
				y_hat = np.sum(y_hat, axis=1)
				print(J, j, theta, y_hat)'''
				X = np.array(X)
				Y = np.array(Y)

				model_linReg.fit(X, Y)

				# Predict values
				
				years_pred = []
				for i in range(2024, 2029):
					years_pred.append([i])

				pred = model_linReg.predict(np.array(years_pred))
				print("Historical: ", Y ,"Predictions:",pred)
				

				#slope, intercept, pred = simple_linear_regression(X, Y,"int","float", [2024, 2029])
				#print(aircraftTypeKey,aircraftTypeDesc, slope, intercept)
				
				#vals = Y+pred
				#years = X+years_pred
				#print(X, years_pred)
				#fig.add_trace(go.Scatter(x=years, y=vals, mode='lines', name=aircraftTypeDesc))
			except Exception as e:
				print(e)
				continue
######################################################################################
# Update layout
fig.update_layout(title='Passenger Flows at Different Airports (2010-2019)',
                  xaxis_title='Year',
                  yaxis_title='Passenger Flow',
                  legend=dict(x=0, y=1, traceorder="normal"))
# Show the plot
fig.show()
#####################################################################################
write_dataOut.close()
######################################################################################
#print(len(unique_aircraftTypeKeys))
