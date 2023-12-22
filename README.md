# Airport Analysis Tools
This repo contains a collection of tools for analyzing operational metrics and service offerings of airports. These tools are designed to provide insights into the operating behavior of airports including the following:
- Passenger thruput by: Origin/Destination and/or O/D Route Combination
- Passenger thruput by: Carrier
- Passenger thruput by: Aircraft Type
- Passenger thruput by: Distance Group
- Service offerings (amenities, concessions, passenger services)


## Scripts
### airportFleetMixYoy.py
- Conducts year over year analysis of aircraft types used by carriers at various airports. The objective of this tool is to determine how much a fleet mix at each airport has changed over time and flag airports that have seen significant changes. The tool measures change in the utilization of a particular aircraft type based on the percentage the airports total annual passenger thruput who flew on that particular aircraft type. For example, if in 2012 at ORD, 25% of ORD's total passenger thruput flew on the B737 and in 2022, 29% of ORD's total passenger thruput  flew on the B737, the B737 will be flagged with a notable increase in utilization at ORD. Significant shifts in the types of aircrafts utilized at an airport can indicate whether the airport may be in need of capacity expansion as larger aircraft with greater seat capacity may exceed the thruput/flight capacity for which the airport was originally designed. Airports with significant changes in fleet mix are flagged and can be filtered in BI tools such as Power BI using the output data.


### allAirportOperationalStats.py & selectAirportOperationalStats.py
- These tools aggregate and summarize the operational metrics of any airport in the datasets or for a single selected airport. The tools aggregate operational metrics by airport, then by carrier, then by aircraft type, then by distance group. At each level of aggregation, a summary of the data is provided so stats do not need to be recalculated in BI or custom visualization tools.
