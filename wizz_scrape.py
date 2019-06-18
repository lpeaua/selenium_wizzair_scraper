#!/usr/bin/env python3
import os
import requests
import pymysql
from decimal import Decimal
import datetime
import csv
import json
import re
import pdb
import random

DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'wizzair'

DB_FILENAME = 'wizzair_allflights_allfares_kwi_cze18.db'

proxy_list = ['193.182.165.43:1212', '185.158.106.2:1212', '77.237.228.188:1212', '185.158.107.85:1212', '77.237.228.205:1212', '185.143.229.57:1212', '185.158.107.55:1212', '31.187.66.160:1212', '77.237.228.140:1212', '192.36.168.89:1212']
random_proxy = random.choice(proxy_list)
PROXIES = {'http': random_proxy}

class WizzairScraper:
    connection = 0
    def __init__(self):
        self.create_db()
        self.read_all_wizz_flights()

    def read_all_wizz_flights(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        all_wiz_flights_data = os.path.join(script_dir, 'wizz_flights_all.csv')
        self.flights_data = []
        f = open(all_wiz_flights_data, 'rt')
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            self.flights_data.append((row[0],row[1],))

    def create_db(self):
        self.dbconn = pymysql.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
        self.dbcur = self.dbconn.cursor()
        self.dbcur.execute('create table if not exists wizzair_flights_14d(retrievaldate date, from_airp varchar(5), to_airp  varchar(5), flightdate date, flighttime varchar(10), price numeric(7,2), currency varchar(10), bundle varchar(10))' )
        self.dbconn.commit()

    def scrape_data(self):
        request_url = 'https://wizzair.com/static/metadata.json'
        #request_url = 'https://be.wizzair.com/9.0.1/Api/search/search'
        url_request = requests.get(request_url, proxies=PROXIES, timeout=20)
        #url_request_json = url_request.json()
        #url = url_request_json['apiUrl'] + str('/search/search')
        url = 'https://be.wizzair.com/9.0.1/Api/search/search'
        for from_airp, to_airp in self.flights_data:
            checkdate = datetime.date.today() + datetime.timedelta(days=14)
            flights_scraped = self.scrape_fares(from_airp, to_airp, checkdate, url)
            WizzairScraper.connection += 1
            print ("connection ", WizzairScraper.connection)
            if flights_scraped == 0:
                print("No flight data available")

    def scrape_fares(self, from_airp, to_airp, checkdate, url):
        headers = {
        	'Content-Type': 'application/json',
        }

        data = """{
        	"isFlightChange":false,
        	"isSeniorOrStudent":false,
        	"flightList":[{
        			"departureStation":"%s",
        			"arrivalStation":"%s",
        			"departureDate":"%02d-%02d-%02d"
        			}
        	],
        	"adultCount":1,
        	"childCount":0,
        	"infantCount":0,
        	"wdc":false,
        	"rescueFareCode":""}""" % (from_airp, to_airp,checkdate.year, checkdate.month, checkdate.day)
        
        print(str(from_airp) + '->' + str(to_airp))
        
        try:
            r = requests.post(url, data=data, headers=headers, proxies=PROXIES, timeout=20)
            if r.status_code == 200:
                flight_data = r.json()
                total_flights = len(flight_data['outboundFlights'])
                if total_flights != 0:
                    arrivalDateTime = flight_data['outboundFlights'][0]['arrivalDateTime']
                    departureDateTime = flight_data['outboundFlights'][0]['departureDateTime'] 
                    match = re.search('\d{4}-\d{2}-\d{2}', departureDateTime)
                    flight_date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
                    flight_time = departureDateTime[-8:]
                    total_fares = len(flight_data['outboundFlights'][0]['fares'])
                    for i in range(0,total_fares):
                        bundle = flight_data['outboundFlights'][0]['fares'][i]['bundle']
                        print(str(from_airp) + '->' + str(to_airp) + '. Flight Time and date: ' + str(departureDateTime))
                        currencyCode = flight_data['outboundFlights'][0]['fares'][i]['fullBasePrice']['currencyCode']
                        price = flight_data['outboundFlights'][0]['fares'][i]['fullBasePrice']['amount']
                        
                        self.dbcur.execute('insert into wizzair_flights_14d(retrievaldate,from_airp,to_airp,flightdate, flighttime, price, currency, bundle) values(%s,%s,%s,%s,%s,%s,%s,%s)',
                                                   (datetime.date.today(), from_airp, to_airp, flight_date, flight_time, Decimal(price), currencyCode, bundle,))
                        self.dbconn.commit()
                    return total_fares;
                else:
                    return 0;
            else:
                return 0;
        except requests.exceptions.Timeout:
            print('No response from server. Request timed out. Retrying now..')
            r = requests.post(url, data=data, headers=headers, proxies=PROXIES, timeout=20)
            if r.status_code == 200:
                flight_data = r.json()
                total_flights = len(flight_data['outboundFlights'])
                if total_flights != 0:
                    arrivalDateTime = flight_data['outboundFlights'][0]['arrivalDateTime']
                    departureDateTime = flight_data['outboundFlights'][0]['departureDateTime'] 
                    match = re.search('\d{4}-\d{2}-\d{2}', departureDateTime)
                    flight_date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
                    flight_time = departureDateTime[-8:]
                    total_fares = len(flight_data['outboundFlights'][0]['fares'])
                    for i in range(0,total_fares):
                        bundle = flight_data['outboundFlights'][0]['fares'][i]['bundle']
                        WizzairScraper.connection += 1
                        print ("connection ", WizzairScraper.connection)
                        print(str(from_airp) + '->' + str(to_airp) + '. Flight Time and date: ' + str(departureDateTime))
                        currencyCode = flight_data['outboundFlights'][0]['fares'][i]['fullBasePrice']['currencyCode']
                        price = flight_data['outboundFlights'][0]['fares'][i]['fullBasePrice']['amount']
                        
                        self.dbcur.execute('insert into wizzair_flights_14d(retrievaldate,from_airp,to_airp,flightdate, flighttime, price, currency, bundle) values(%s,%s,%s,%s,%s,%s,%s,%s)',
                                                   (datetime.date.today(), from_airp, to_airp, flight_date, flight_time, Decimal(price), currencyCode, bundle,))
                        self.dbconn.commit()
                    return total_fares;
                else:
                    return 0;
            else:
                return 0;

if __name__ == '__main__':
    wizzairscraper = WizzairScraper()
    wizzairscraper.scrape_data()
