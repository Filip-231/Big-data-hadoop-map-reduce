

from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag
import re
WORD_RE=re.compile(r"[\w]+")

lemmatizer=WordNetLemmatizer()
stop_words=stopwords.words('english')
''' 
python 04_top_20_common_adjective.py test.txt
python 04_top_20_common_adjective.py prep_1.tsv

UWAGA URUCHAMIANIE PLIKU W CHMURZE:
po dodaniu .mrjob.conf

runners:
  emr:
    aws_access_key_id:
    aws_secret_access_key: 
    region: eu-west-1
    num_core_instances: 4
    bootstrap:
    - sudo pip3 install --user -U nltk singledispatch
    - sudo python3 -m nltk.downloader -d /usr/share/ntlk_data all

do katalogu C users user
i usunieciu folderu .aws

python 04_top_20_common_adjective.py -r emr prep_reviews.csv --output-dir=s3://big-data-231/output/job-from-pycharm

python 04_top_20_common_adjective.py -r  emr s3://big-data-231/data/prep_reviews.csv --output-dir=s3://big-data-231/output/job-from-pycharm


python 04_top_20_common_adjective.py -r emr test.txt --output-dir=s3://big-data-231/output/job-from-pycharm


'''
# import nltk as n
# n.download('wordnet')
# n.download('stopwords')
# n.download('averaged_perceptron_tagger')


class MRFood(MRJob):

    def steps(self):
        return[
            MRStep(mapper=self.mapper),
            MRStep(mapper=self.mapper_get_keys,reducer=self.reducer),
            MRStep(mapper=self.mapper_get_1_and_5,reducer=self.reducer_get_20_words)
        ]
    # def maper_init(self):
    #     import nltk as n
    #     n.download('wordnet')
    #     n.download('stopwords')
    #     n.download('averaged_perceptron_tagger')

    def mapper(self,_,line):
        (Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,
         Score,Time,Summary,Text)= line.split('\t')

        words=WORD_RE.findall(Text)
        #filtering when len word is > 1
        words=filter(lambda word: len(word) > 1,words)
        words=map(str.lower,words) #make lower letters
        words=map(lemmatizer.lemmatize,words) # mice=> mouse
        words=filter(lambda word: word not in stop_words,words)

        for word in words:
            if pos_tag([word])[0][1]=='JJ': #two dimensional tuple
                yield Score, word



    def mapper_get_keys(self,key,value):
        #["4", "good"]   1
        yield (key,value),1

    def reducer(self,key,values):
        yield key, sum(values)
    def mapper_get_1_and_5(self,key,value):
        if key[0]=='1':
            yield key[0],(key[1],value)
            #key[0] is score, key[1] is number of values
        if key[0]=='5':
            yield key[0],(key[1],value)

    def reducer_get_20_words(self,key,values):
        results={}
        for value in values:
            results[value[0]]=value[1]
            # i made dict for words with 1 and 5 score and their numbers
        sorted_results=sorted([(val,key) for key,val in results.items()],reverse=True)
        # i made list of words from dict, on first elem is number


        yield key, sorted_results[:20]

if __name__=='__main__':
    MRFood.run()