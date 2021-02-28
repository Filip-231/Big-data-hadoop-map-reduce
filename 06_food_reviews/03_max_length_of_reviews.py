from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE=re.compile(r"[\w]+")

''' 
python 03_max_length_of_reviews.py test.txt
python 03_max_length_of_reviews.py prep_reviews.csv
'''

class MRFood(MRJob):

    def mapper(self,_,line):
        (Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,
         Score,Time,Summary,Text)= line.split('\t')

        words=WORD_RE.findall(Text)

        yield None, len(words)


    def reducer(self,key,values):

        yield key, max(values)

if __name__=='__main__':
    MRFood.run()