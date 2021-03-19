import pandas
#data clean
df = pandas.read_csv('day29.csv')
df.columns=['car','cross','timestamp']
dff=df.drop_duplicates()
dfff = dff.groupby("car").filter(lambda x: (len(x)>=54))

#cross delete
loc=dfff['cross'].value_counts()
loc=loc[:5].keys()
dfff=dfff.loc[~dfff['cross'].isin(loc)]

dfff.to_csv('nday1.csv',index = False,header=None,mode='a')

