#!/usr/bin/python3

##############################################################
#
# Merchant is a suite of tools meant to provide actionable
# local market data through analysis of Craigslist ads.
# This is achieved by with three groups of functions:
#
# 1 - Scraper: scrapes the local Craigslist page for all items,
# locations and pricing. This information is then uploaded
# to a SQLite database for analysis.
#
# 2 - Analytics engine: Queries SQLite database for
# item information and attempts to infer relationships
# between asking price, time on market, sales price and item
# type or location. This includes plotting these relationships
# to better visualize correlation and distribution.
#
# 3 - Support: Functions that provide useful transformations
# or other tasks which are expected to be used infrequently.
# These include classifying item types from headlines.
#
###############################################################

#########TO DO################
# DB
# Link-table with categories and items
# Scraper
# -Insert/Update data * needs shell output
#
# Analytics Engine:
# -Testing
# -Filter junk data ($1 cars, etc.)
# -Other metrics (location?)
# -Regression curve on scatterplot
#
# Notifier (maybe separate program that calls scraper?)
#
# Support
# -Item classifier * needs shell output
##############################

import sqlite3

###########GLOBALS############
base_url = 'https://littlerock.craigslist.org'
db = 'Craigslist.db'
##############################

class LiteDB:
    '''
    The LiteDB class contains methods for common insert and update satements.
    It also holds methods for creating our tables.
    '''

    def __init(self):
        self.nothing = 'nothing'
        # nothing . . . yet!

    def initialize_db(self,cursor):
        # Create the main table for listings
        query_string = '''
        CREATE TABLE listings (cid INTEGER PRIMARY KEY, url TEXT, post_date DATE,
        title TEXT, price INTEGER, area TEXT, cat_id INTEGER, item_id INTEGER,
        location TEXT, last_seen DATE)
        '''
        cursor.execute(query_string)
        # Create the category and item tables
        cursor.execute('CREATE TABLE category (id INTEGER PRIMARY KEY ASC,\
                       name TEXT)')
        cursor.execute('CREATE TABLE items (id INTEGER PRIMARY KEY ASC,\
                       name TEXT)')
        # Create two rows in categories to handle bad matches
        cursor.execute('INSERT INTO items (name) VALUES (?)',('no group','multi group'))        return True
        print('Database tables initialized')
        return True
    
    def upload(self,data):
        #check for 


class Scraper:
    '''
    This class contains all methods necessary to make requests
    of the craigslist website and insert or update the Merchant
    database.
    '''
    import requests
    import bs4
    import pickle
    import subprocess

    def __init__(self):
        self.base_url = base_url
        self.cat_codes = pickle.load(open('class codes','rb'))

    ## TO DO: integrate Demetrius

    def scrape_category(self,category):
        results = list()
        req = requests.get(base_url+self.cat_codes[category])
        soup = bs4.BeautifulSoup.get(req.text,'lxml')
        num_ads = int(soup.find('span',{'class':'totalcount'}).text)
        num_pages = round(num_ads/120)
        pg_url = base_url+self.cat_codes[category]+"?s="
        for i in range(num_pages):
            page_results = scrape_page(pg_url+str(i*120))
            results += page_results
        return results

    def scrape_page(self,url):
        page_results = list()
        req = requests.get(url)
        soup = bs4.BeautifulSoup.get(req.text,'lxml')
        ads = soup.find('ul',attrs={'class':'rows'})
        rows = ads.find_all('li')
        # Assign values to the variables
        for row in rows:
            row_data = dict() # container for relevant key-value pairs in the HTML
            row_data['cid'] = row['data-pid'] # Craigslist-ID, our primary key
            price = row.find('span',{'class':'result-price'}).text
            row_data['price'] = int(price.strip('$')) # Cast as an integer
            row_data['title'] = row.find('a',{'class':'result-title hdrlnk'}).text
            location = row.find('span',{'class':'result-hood'}).text
            row_data['location'] = location.strip().strip('()') # Strip whitespace and parenthesis
            row_data['post date'] = row.find('time')['datetime']
            url = row.find('a',{'class':'result-title hdrlnk'})['href']
            row_data['url'] = self.base_url+url
            page_results.append(row_data)
        return page_results
            
            
    
class AnalyticsEngine:
    '''
    This class contains methods for statistical methods and
    visualization of market data. This will eventually support
    a notifier class of methods which will provide email or
    SMS notification to a user.
    '''

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt

    def __init__(self,category,item):
        self.bins = 20 # number of bins in our histograms
        self.threshold = 20 # Filter postings below this price
        self.category = category
        self.item = item
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()
        c = self.cursor.execute('SELECT id FROM category WHERE name=?',category)
        self.cat_id = c.fetchone()
        c = self.cursor.execute('SELECT id FROM items WHERE name=?',item)
        self.item_id = c.fetchone()

    def get_data(self):
        # Selects prices with matching category and item ID numbers
        # and returns a dataframe of those values.
        query = '''
        SELECT price, post_date, last_seen FROM listings INNER JOIN category ON category.{cat_id}
        = listings.cat_id INNER JOIN items ON items.{item_id} = listings.item_id
        '''
        df = pd.read_sql(query.format(cat_id = self.cat_id, item_id= self.item_id),
                         con=self.cursor)
        # Get rid of rows with prices below the threshold
        df = df[df['price'] > self.threshold]
        return df

    def mean_ask(self,df):
        # Returns mean asking price for entire dataset
        mean = df['price'].mean()
        return mean
        
    def gen_hist_price(self,df):
        # Saves a histogram of ask price distribution
        df.plot.hist(df['price'],self.bins)
        plt.save_fig('plots/hist/%s.%s' % (self.category,self.item))
        return True

    def gen_scatter(self,df):
        # Saves a scatter plot of ask price vs listing lifespan
        df['delta'] = df['last_seen']-df['post_date']
        df.plot.scatter(df['delta'],df['price'])
        plt.save_fig('plots/scatter/%s.%s' % (self.category,self.item))
        return True


class Support:
    '''
    This class contains miscellaneous methods which are not expected
    to be used frequently. Provided primarily as a logical rather
    than functional grouping.
    '''
    
    import pickle
    from nltk import word_tokenize,pos_tag
    import pandas as pd

    def __init__(self):
        self.class_codes = pickle.load(open('class codes','rb'))
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()

    def populate_categories(self):
        # insert category names into our table
        keys = [key for keys in self.class_codes]
        self.cursor.executemany('INSERT INTO category (name) VALUES (?)',keys)
        self.conn.commit()
        print('Categories updated')
        return True

    def create_item_list(self, results):
        # Reads title strings and determines probable groups of items
        # by identifying most common nouns in title strings.
        # This method should not be expected to correctly identify every
        # logical grouping of items. New groupings should be inserted
        # into the item table as they become apparent.
        print('Tokenizing title strings . . .')
        titles = [row['title'].lower() for row in results]
        titles = [word_tokenize(title) for title in titles]
        titles = [pos_tag(title) for title in titles] # Returns tuples of words and part of speech
        nouns = list() # Container for pandas series object
        for title in titles: # Find each tuple tagged as a noun and append
            for tag in title:
                if tag[1] == 'NN':
                    nouns.append(tag)
        items = list() # This will hold results for the insert statement
        s = pd.Series(nouns) # Convert noun-list to pandas series for convenience
        for i in s.value_counts().index[:20]: # Get up to 20 most common nouns
            print('Is %s a valid item type? y/n' % i) # Let user validate item name
            responded = False
            while not responded:
                user_response = raw_input('>')
                if user_response == 'y':
                    items.append(i)
                    responded = True
                elif user_response == 'n':
                    responded = True
                else:
                    print('y or n only, please')
        # Insert into SQLite
        for item in items:
            self.cursor.execute('SELECT 1 FROM items WHERE name = (?)',item)
            test = self.cursor.fetchone()
            if test:
                self.cursor.execute('INSERT INTO items (name) VALUES (?)',item)
        self.conn.commit()
        print('Item table updated')
        return True
        
