from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordCount(MRJob):
    '''
    python 01_word_count.py iliada.txt
    '''
    def mapper(self,_,line):
        words=line.split() #list of words in each line

        for word in words:# for every word in every line
            yield word.lower(),1

    def reducer(self,key,values):
        yield key, sum(values)


if __name__=='__main__':
    MRWordCount.run()