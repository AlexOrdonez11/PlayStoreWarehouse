from DBLoader import App
import pandas as pd
import os
import numpy as np
import re

def clean_illegal_chars(value):
    if isinstance(value, str):
        return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', value)
    return value

def CvsInsert(df, string,num):
    df = df.applymap(clean_illegal_chars)
    file_path_os ="./data/"+ string +"_"+str(num)
    if os.path.exists(file_path_os+ ".csv"):
        df.to_csv(file_path_os +".csv", mode='a', header=False,index=False)
    else:
        df.to_csv(file_path_os + ".csv", mode='a', header=True, index=False)
    

if __name__== "__main__":
    df= pd.read_csv("./top5_cleaned.csv")
    num=11
    print(num)
    df = np.array_split(df,12)[num]
    for i, row in df.iterrows(): 
        app=App(row['App Id'])
        print(app.appId)
        try:
            CvsInsert(app.getApp(),"AppInformation",num)
            CvsInsert(app.getReviews(),"Reviews", num)
        except:
            print("error with:" + app.appId)
            continue
    

    df_excel= pd.read_csv("./data/AppInformation_"+str(num)+".csv")
    df_excel.to_excel("./data/AppInformation_"+str(num)+".xlsx",index=False)
    df_excel= pd.read_csv("./data/Reviews_"+str(num)+".csv")
    df_excel.to_excel("./data/Reviews_"+str(num)+".xlsx",index=False)




