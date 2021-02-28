import pandas as pd
df=pd.read_csv('Reviews.csv')

print(df.info()) #informations about data frame


#writing to new file
df.to_csv('prep_reviews.csv',sep='\t',header=False,index=False) #no header no indexes