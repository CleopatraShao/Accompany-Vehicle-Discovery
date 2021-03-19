import pandas
df = pandas.read_csv('xbj')
df.columns=['car','cross','timestamp']
a=1420041600
for y in range(31):
    dfe = df.loc[(df['timestamp']>=a+86400*y)&(df['timestamp']<a+86400*(y+1))]
    filename = 'day' + str(y+1) + ".csv"
    dfe.to_csv(filename,index = False,header=None,mode='a')

