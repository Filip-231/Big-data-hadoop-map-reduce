import re


#\w word character a-z 0 -9 _,
#looking for, only one elem
WORD_RE=re.compile(r'[\w]+')

words=WORD_RE.findall('Big data, hadoop and map reduce. (hello word!)')
print(words)

