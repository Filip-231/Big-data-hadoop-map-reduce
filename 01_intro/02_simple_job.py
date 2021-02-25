from mrjob.job import MRJob

class MRSimpleJob(MRJob):
    def mapper(self,_,value):
        yield