from mrjob.job import MRJob
from mrjob.step import MRStep

"""
python 03_average_distance_job.py test.txt
python 03_average_distance_job.py preprocessed_data.csv
cls - cleaning terminal

"""

class MRFlights(MRJob):
    def steps(self):
        #controlling steps of our job
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self,_, line):
        #building maper which reads line by line

        # test data:
        # "2015"[1, 1, "AA", 2330]
        year, items= line.split('\t') #divide by tabular

        year=year[1:-1] #bs year was like: \"2015\"
        items=items[1:-1]
        month, day, airline, distance= items.split(', ')

        # yield year, (month, day, airline, distance)
        distance=int(distance)
        yield None, distance # everywhere I have year 2015 so None


    def reducer(self,key,values):
        total= 0
        num_elements=0

        for value in values:
            total += value
            num_elements+=1

        yield None, total/ num_elements



if __name__ == '__main__':
    MRFlights.run()