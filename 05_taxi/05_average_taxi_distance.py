

from mrjob.job import MRJob
from mrjob.step import MRStep
''' 
python 05_average_taxi_distance.py test.txt
python 05_average_taxi_distance.py yellow_tripdata_2016-01.csv > average_taxi_dist.csv
'''

class MRTaxiTrip(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper,reducer=self.reducer)
        ]



    def mapper(self,_,line):
        (VendorID,tpep_pickup_datatime, tpep_dropoff_datatime, passenger_count,trip_distance, pickup_longitude,
         pickup_latitude,RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,total_amount)= line.split(',')

        trip_distance=float(trip_distance)

        yield 1, trip_distance



    def reducer(self,key,values):
        num=0
        sum=0
        for value in values:
            sum+=value
            num+=1


        yield None, sum/num

if __name__=='__main__':
    MRTaxiTrip.run()