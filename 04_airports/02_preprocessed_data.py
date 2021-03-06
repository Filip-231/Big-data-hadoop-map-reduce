from mrjob.job import MRJob
from mrjob.step import MRStep

"""
python 02_preprocessed_data.py flights.csv


writing to a file:
python 02_preprocessed_data.py flights.csv > preprocessed_data.csv
"""

class MRPreprocess(MRJob):

    def mapper(self,_,line):
        #mapping every line to a variable
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT,
         DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT,
         WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,
         SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON,
         AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY)=line.split(',')
        MONTH,DAY,DISTANCE=int(MONTH),int(DAY),int(DISTANCE)
        yield  YEAR, (MONTH,DAY,AIRLINE,DISTANCE) #need to be: key - value



if __name__ == '__main__':
    MRPreprocess.run()