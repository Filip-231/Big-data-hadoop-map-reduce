from mrjob.job import MRJob
from mrjob.step import MRStep

''' 
python 04_total_pickups_by_hour.py test.txt
python 04_total_pickups_by_hour.py yellow_tripdata_2016-01.csv > pick_up_by_hour.csv
'''


class MRTaxi(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datatime, tpep_dropoff_datatime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')

        ##tpep_pickup_datatime=tpep_pickup_datatime[11:13]
        hour = tpep_pickup_datatime.split()[1][:2]

        yield hour, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRTaxi.run()


