import twint
import pandas as pd
from datetime import timedelta
from datetime import datetime
import nest_asyncio
nest_asyncio.apply()

c        = twint.Config()
c.Lang   = "en"
c.Search = '#tesla'
c.Pandas = True
c.limit  = 1000
try:
    df       = pd.read_csv('main_df.csv')
    c.Until  = str(datetime.strptime(df.iloc[-1]['date'], "%Y-%m-%d %H:%M:%S").date())
    print("UNTIL DATE : ",c.Until) 

    twint.run.Search(c)

    data     = twint.storage.panda.Tweets_df 
    print("df: "  , df.shape)
    print("data: ", data.shape)

    if data.shape[0]==0:
        df.at[-1,'date']            =      str((datetime.strptime(df.iloc[-1]['date'], "%Y-%m-%d %H:%M:%S")-timedelta(1)))
        df.to_csv('main_df.csv', index=False)
    else: 
        df = pd.concat([df, data])
        df.to_csv('main_df.csv', index=False)
        
except:
    print(" No master df found")
    twint.run.Search(c) 
    data     = twint.storage.panda.Tweets_df 
    data     = data.to_csv('main_df.csv', index=False)





