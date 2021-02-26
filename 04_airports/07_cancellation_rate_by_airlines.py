from mrjob.job import MRJob
from mrjob.step import MRStep

"""
Which airline cancel flights mostly?


python 07_cancellation_rate_by_airlines.py flights.csv --airlines airlines.csv> cancellation_rate.csv
"""


class MRFlight(MRJob): # ctrl + click code of class
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init, # task to do before reducer
                   reducer=self.reducer)

        ]
    def mapper(self, _, line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT,
         DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT,
         WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,
         SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON,
         AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')
        yield AIRLINE, int(CANCELLED)

    def reducer(self, key, values):
        total = 0
        num_rows = 0
        for value in values: #0 or 1
            total += value
            num_rows+=1
        yield self.airline_name[key], total / num_rows

    def reducer_init(self):
        self.airline_name={}
        with open('airlines.csv','r') as file:
            for line in file:
                code, full_name=line.split(',')
                full_name=full_name[:-1]
                self.airline_name[code]=full_name

    def configure_args(self):
        #overwriting function to get new arguments about names of arilines
        super(MRFlight,self).configure_args()
        #adding path to a file
        self.add_file_arg('--airlines',help='Path to the airlines.csv')

if __name__ == '__main__':
    MRFlight.run()
