from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from dataclasses import dataclass,field
from time import sleep
import re
from requests_html import HTML



def get_user_agent():
    return UserAgent(verify_ssl=False).random

# create city radius database 
class CityRadius:
    def __init__(self,property,listOfIDs) -> None:

        self.cityRadiusData = (property,listOfIDs)

    @property
    def cityRadiusData(self):
        return self._cityRadiusData
     
    @cityRadiusData.setter
    def cityRadiusData(self,vals):  
        '''creates local (instance)) city radius dictionary with item id key based on data from database 
        cityRadius data  = [city : str,cityradius :int]''' 
        radiusData = {}
        prop,listOfIDs = vals
        if listOfIDs != []:
            for id in listOfIDs:
                try:
                    r = list(prop.objects().filter(id=id).values_list('city','cityRadius'))
                    radiusData[id] = r[0]
                except: IndexError # data does not exist 
        self._cityRadiusData = radiusData
    
    def getEntryData(self,id,queryCity,queryRadius):
        if id in self.cityRadiusData and self.cityRadiusData[id][0] == queryCity:
            if queryRadius < self.cityRadiusData[id][1]: 
                return queryRadius
            else:
                return self.cityRadiusData[id][1]
        else:
            return queryRadius

@dataclass
class QueryData:
    propertyType : int
    city : str
    cityRadius: int        
    price : float = field(default=None)
    pageNo : int = field(init=False,default = 1)
    url : str = field(init=False,default = "")

    def __post__init(self):
        self.url : str = self.createUrl()
    
    def queryPage(self):
        return f"page={self.pageNo}"
    
    def queryPriceMax(self): 
        if self.price is None : 
            return ""
        return f"&priceMax={self.price}"
    
    def queryCity(self):
        return f"{self.city}?distanceRadius={self.cityRadius}"

    def queryPropertyType(self):

        textDataDict ={
            "mieszkanie":[""],
            "dom":["DETACHED","SEMI_DETACHED","RIBBON"],
            "dzialka":["BULDING"]
        }

        match self.propertyType//10:  
            case 1 : 
                propertyTypeText =  "mieszkanie" 
                queryText =""               
            case 2 : 
                propertyTypeText =  "dom"
                queryText = "&buildingType="+textDataDict[propertyTypeText][self.propertyType%10]
            case 3 : 
                propertyTypeText =  "dzialka"
                queryText = "&plotType="+textDataDict[propertyTypeText][self.propertyType%10]
            case _ :
                raise ValueError("Wrong property type")
        try:
            return (propertyTypeText,queryText)
        except ValueError or IndexError:
            print("Wrong property type")
    

    
    def queryUpdate(self):
        self.pageNo+=1
        self.url = self.createUrl()
           
    
    def createUrl(self): 
        queryPropertyType = self.queryPropertyType()[0]
        queryPropertyTypeDetais = self.queryPropertyType()[1]
        url = f'https://www.otodom.pl/pl/oferty/sprzedaz/{queryPropertyType}/{self.queryCity()}&{self.queryPage()}&limit=72&ownerTypeSingleSelect=ALL{queryPropertyTypeDetais}&direction=DESC&viewType=listing{self.queryPriceMax()}'
        return url
    
    def __str__(self):
        return self.url

class Scraper:
    objects = {}
    def __init__(self,query,endless_scroll) -> None:
        self.query : QueryData = query
        self.endless_scroll : bool = endless_scroll
        self.endless_scroll_time : int = 5 
        self.driver : WebDriver = None 
        self.scrapedData : list = []
        self.scrapedListOfIDs: list = []
        self.newScrapedData : list = []
        self.newScrapedListOfIDs: list = []
        Scraper.objects[id(self)]= self


    def get_driver(self):
        if self.driver is None:
            user_agent = get_user_agent()  
            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--headless=new")
            options.add_argument("user-agent={user_agent}")
            self.driver = webdriver.Chrome()
        return self.driver
    
    def perform_endless_scroll(self,driver):
        if self.endless_scroll:
            current_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
                sleep(self.endless_scroll_time)
                iter_height = driver.execute_script("return document.body.scrollHeight") 
                if iter_height == current_height:
                    break
                current_height = iter_height

    def get(self):
        driver = self.get_driver()
        driver.get(self.query.createUrl())
        if self.endless_scroll:
            self.perform_endless_scroll(driver)
        return driver.page_source
 
    def checkNewScrapedData(self):
        '''checks if new scraped data are empty'''
        return self.newScrapedData == [] and self.newScrapedListOfIDs ==[]
    
    def scrapeSinglePage(self,**kwargs):
        data =[]
        listOfIDs =[]
        # get page source and createt html object
        pageSource = self.get()
        html_obj = HTML(html = pageSource)
        # look only for organic list, skip promoted ones
        listOrganic = html_obj.find("[data-cy='search.listing.organic']")
        # check if the page contains data, if not - finish scraping
        if listOrganic == []: 
            self.newScrapedData,self.newScrapedListOfIDs = data,listOfIDs
            return
        items = listOrganic[0].find("[data-cy='listing-item']")


        for item in items:

            # init data
            dataset ={}
            row=[]
            # get id
            ahref = item.find("[data-cy='listing-item-link']",first=True).attrs["href"]
            id = re.search("(?<=-ID){1}(.+)", ahref).group()
            # get title
            try:
                title = item.find("article>p",first= True).element.text_content()
            except:
                continue
        #     print(title)
            row.append(title)

            #get rest of the data from spans
            articles = item.find("article")
            for article in articles:
                spans = article.find("div>span")
                for span in spans:
                    content =""
                    try:
                        content = span.element.text_content()
                    except:

                        pass

                    if content !="":
                        _content = content.strip()
                        row.append(_content)

                try:
                    dataset["id"]=id
                    dataset["title"]=title
                    dataset["city"] = self.query.city
                    dataset["cityRadius"]  = self.query.cityRadius
                    dataset["propertyType"]=self.query.propertyType
                    dataset["price"] = float(row[1].split("\xa0zł")[0].replace("\xa0",""))
                    dataset["size"] = float(row[4].split(" m²")[0])
                    data.append(dataset)
                    listOfIDs.append(id)
                except ValueError:
                    continue
        #update data 
        self.newScrapedData,self.newScrapedListOfIDs = data,listOfIDs
        self.scrapedData += data
        self.scrapedListOfIDs += listOfIDs

    def radiusUpdate(self,property):
        # get radius data from cassandra
        cityRadius = CityRadius(property=property,listOfIDs=self.scrapedListOfIDs)     

        # update radius if it was already in database 
        for i in range(len(self.scrapedData)):
            id = self.scrapedListOfIDs[i]
            self.scrapedData[i]["cityRadius"] = cityRadius.getEntryData(id,self.scrapedData[i]["city"],self.scrapedData[i]["cityRadius"])
        