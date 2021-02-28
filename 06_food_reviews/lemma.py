from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag #part of speach tag

import nltk as n
# n.download('wordnet')
# n.download('stopwords')
n.download('averaged_perceptron_tagger')

lemmatizer=WordNetLemmatizer()
stop_words=stopwords.words('english')


#
print(lemmatizer.lemmatize('computers'))
print(lemmatizer.lemmatize('mice'))
print(lemmatizer.lemmatize('books'))

print(stop_words)

#adjectives
#JJ
print(pos_tag(['big']))  #mapping words to code
print(pos_tag(['book'])[0][1]) #NN


print(pos_tag(['good'])[0][1]) #JJ
print(pos_tag(['andrzej'])[0][1]) #NN