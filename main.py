from DBLoader import App
import pandas as pd
import os
import numpy as np

def CvsInsert(df, str):
    file_path_os ="./data/"+ str +".xlsx"
    if os.path.exists(file_path_os):
        df.to_excel("./data/"+ str +".xlsx", mode='a', header=False,index=False)
    else:
        df.to_excel("./data/"+ str +".xlsx", mode='a', header=True, index=False)

if __name__== "__main__":
    df= pd.read_csv("./top5_cleaned.csv")
    df = np.array_split(df,12)[1]
    for i, row in df.iterrows(): 
        app=App(row['App Id'])
        print(app.appId)
        CvsInsert(app.getApp(),"AppInformation")
        CvsInsert(app.getReviews(),"Reviews")




