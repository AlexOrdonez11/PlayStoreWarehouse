from DBLoader import App
import pandas as pd



if __name__== "__main__":
    app=App("")
    app.getApp()
    app.getReviews(num=3)

def CvsInsert(df, str):
    df.to_csv("./data/"+ str +".csv")


