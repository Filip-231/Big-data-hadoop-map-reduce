from mrjob.job import MRJob
from mrjob.step import MRStep
''' 
python 02_average_total_amount_by_vendor.py yellow_tripdata_2016-01.csv > passenger_count.csv

number of courses by number of passangers
'''

class MRNumber_vendor(MRJob):

    def mapper(self,_,line):
        (VendorID,tpep_pickup_datatime, tpep_dropoff_datatime, passenger_count,trip_distance, pickup_longitude,
         pickup_latitude,RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,total_amount)= line.split(',')
        yield float(passenger_count),1


    def reducer(self,key,values):
        total=0
        num=0
        for value in values:
            total +=value
            num+=1
        yield key, total

if __name__=='__main__':
    MRNumber_vendor.run()