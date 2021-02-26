from mrjob.job import MRJob
from mrjob.step import MRStep
"""
python 04_average_departure_arrival_delay.py flights.csv > average_delay.csv
"""


class MRFlight(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)

        ]
    def mapper(self,_,line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT,
         DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT,
         WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,
         SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON,
         AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')
        '''
        #some of records are empty so we can't convert to float
        
        try:
            DEPARTURE_DELAY=float(DEPARTURE_DELAY)
            # ARRIVAL_DELAY=float(ARRIVAL_DELAY)
        except:
            yield None, DEPARTURE_DELAY
        '''
        if DEPARTURE_DELAY=='':
            DEPARTURE_DELAY=0
        if ARRIVAL_DELAY=='':
            ARRIVAL_DELAY=0

        DEPARTURE_DELAY=int(DEPARTURE_DELAY)
        ARRIVAL_DELAY=int(ARRIVAL_DELAY)
        MONTH=int(MONTH)

        #          2 places and integer
        yield f'{MONTH:02d}', (DEPARTURE_DELAY, ARRIVAL_DELAY)

    def reducer(self,key,values):
        # if i have key = 5 all values go to one list
        total_dep_delay= 0
        total_arr_delay=0
        num_elements=0
        for value in values:
            total_dep_delay+=value[0] #first elem is delay
            total_arr_delay+=value[1]
            num_elements+=1
        #    MONTH
        yield key, (total_dep_delay/num_elements, total_arr_delay/num_elements)



if __name__== '__main__':
    MRFlight.run()
