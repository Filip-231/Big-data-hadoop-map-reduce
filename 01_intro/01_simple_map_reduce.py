from mrjob.job import MRJob
#ctr+alt+s to see settings

#venv\Scripts\activate

class MRWordCount(MRJob):
    '''
    python 01_simple_map_reduce.py data.txt
    only one process
    Elastic map reduce:
        configuration of awscli
        -r/--runner emr
        $python 01_count_words_job.py -r emr s3://bucket-name/data.txt
    '''
    #I need to define min one step:
    def mapper(self,_,line):
        #generator
        yield 'chars',len(line) #number of chars in text
        yield 'words', len(line.split()) #adding number of words

    def reducer(self,key,values):
        #sum of all values
        yield key, sum(values)



if __name__=='__main__':
    MRWordCount.run()


