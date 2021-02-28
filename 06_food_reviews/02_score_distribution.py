from mrjob.job import MRJob
from mrjob.step import MRStep
''' 
python 02_score_distribution.py test.txt
python 02_score_distribution.py prep_reviews.csv
'''

class MRFood(MRJob):

    def mapper(self,_,line):
        (Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,
         Score,Time,Summary,Text)= line.split('\t')
        yield Score,1


    def reducer(self,key,values):

        yield key, sum(values)

if __name__=='__main__':
    MRFood.run()