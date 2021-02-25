from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSimpleJob(MRJob):
    '''
    python 02_simple_job.py SMSSpamCollection.txt
    '''
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer) #if i remove one of this i will execute only one
        ]



    def mapper(self,_,line):
        yield 'lines', 1 #for every line 1
        yield 'words',len(line.split())
        yield 'chars', len(line)
    def reducer(self,key,values):
        #shuffle and sort

        yield key, sum(values) #summing by a key every line


if __name__=='__main__':
    MRSimpleJob.run()