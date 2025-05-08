from google_play_scraper import app, reviews # type: ignore
import pandas as pd

class App:
    def __init__(self, appId):
        self.appId=appId

    def getApp(self):
        data=app(
            self.appId,
            lang='en',
            country='us'
        )
        df=pd.DataFrame.from_dict([{'AppId':self.appId,
            'Title':data['title'],
            'ContentDescription':data['description'],
            'Installs':data['realInstalls'],
            'Currency':data['currency'],
            'Price':data['price'],
            'OffersInApp':data['offersIAP'],
            'Name':data['developer'],
            'DeveloperId':data['developerId'],
            'Email':data['developerEmail'],
            'Website':data['developerWebsite'],
            'Address':data['developerAddress'],
            'GenreId':data['genreId'],
            'Genre_Name':data['genre'],
            'ContentRating':data['contentRating'],
            'ContainsAds':data['containsAds'],
            'DateReleased':data['released']
        }])
        df2=pd.DataFrame(data['categories'])
        df=pd.concat([df]*(len(df2)),ignore_index=True)
        return pd.concat([df,df2],axis=1)
        


    
    
    def getReviews(self, num=5000):
        data = reviews(
            app_id=self.appId,
            count=num)
        appId=[]
        for i in range(0, len(data[0])):
            appId.append(self.appId)

        rdata=pd.DataFrame(data[0])
        rdata=rdata[['reviewId', 'userName', 'score', 'thumbsUpCount', 'at', 'content']]
        rdata['AppId']=appId

        return rdata

    
    def setApp(self, app):
        self.appId=app
    