from google_play_scraper import app, reviews # type: ignore

class App:
    def __init__(self, appId):
        self.appId=appId

    def getApp(self):
        return app(
            self.appId,
            lang='en',
            country='us'
        )
    
    def getReviews(self, num):
        return reviews(
            app_id=self.appId,
            count=5000)
    