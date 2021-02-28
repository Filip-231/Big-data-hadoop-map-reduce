# preparation orginal data to test locally

import pandas as pd
df=pd.read_csv('prep_reviews.csv',sep='\t',header=None)

df=df[:100000]

df.to_csv('prep_1.tsv',sep='\t',header=False,index=False)