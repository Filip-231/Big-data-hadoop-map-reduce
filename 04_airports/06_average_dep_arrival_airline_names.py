from mrjob.job import MRJob
from mrjob.step import MRStep

"""

python 06_average_dep_arrival_airline_names.py flights.csv --airlines airlines.csv

python 06_average_dep_arrival_airline_names.py flights.csv --airlines airlines.csv > airline.csv

"""


class MRFlight(MRJob): # ctrl + click code of class
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init, # task to do before reducer
                   reducer=self.reducer)

        ]
    def configure_args(self):
        #overwriting function to get new arguments about names of arilines
        super(MRFlight,self).configure_args()
        #adding path to a file
        self.add_file_arg('--airlines',help='Path to the airlines.csv')

    def reducer_init(self):
        #make a dictionary with names
        self.airline_name={}
        with open('airlines.csv','r') as file:
            #step by each line
            for line in file:
                code, full_name=line.split(',')
                full_name=full_name[:-1]
                self.airline_name[code]=full_name




    def mapper(self, _, line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT,
         DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT,
         WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,
         SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON,
         AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')

        if DEPARTURE_DELAY == '':
            DEPARTURE_DELAY = 0
        if ARRIVAL_DELAY == '':
            ARRIVAL_DELAY = 0

        DEPARTURE_DELAY = int(DEPARTURE_DELAY)
        ARRIVAL_DELAY = int(ARRIVAL_DELAY)


        yield AIRLINE, (DEPARTURE_DELAY, ARRIVAL_DELAY)

    def reducer(self, key, values):

        total_dep_delay = 0
        total_arr_delay = 0
        num_elements = 0
        for value in values:
            total_dep_delay += value[0]
            total_arr_delay += value[1]
            num_elements += 1
        # i will use my dictionary
        yield self.airline_name[key], (total_dep_delay / num_elements, total_arr_delay / num_elements)


if __name__ == '__main__':
    MRFlight.run()
