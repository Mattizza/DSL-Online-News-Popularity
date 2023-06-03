
import requests
import re
import time
import numpy as np

from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta


class Scraper():

    def __init__(self):
        '''
        Builds a `Scraper` object. Its goal is to make easier the scraping procedure
        thanks to the methods it provides.
        '''
        
        self = self


    def set_url(self, url: list) -> None:
        '''
        Pass a list `URL`'s to scrape.

        Parameter
        ---
        url : list
            List of `URL`'s, each in a `str` format.
        '''
        
        self.__url__ = url


    def start_driver(self) -> None:
        '''
        Start a `Selenium` webdriver using Firefox as browser.
        '''

        self.__driver__ = webdriver.Firefox()
        print('DRIVER ONLINE')


    def scrape(self, years: list = ['2013', '2014', '2015']) -> dict:
        '''
        Starts the scraping over the `years`.

        Parameters
        ---
        years : list, default = ['2013', '2014', '2015']
            List of the years over which the scraper will work.
        
        Output
        ---
        A dictionary containing the `URL` and the `HTML` associated for each year.

        '''
        
        self.__url_html__ = {}
        print(f"YEARS: {years}")
        print(f"URL: {len(self.__url__)}")
        print(f"\nSTART SCRAPING -- EXPECTED TIME REQUIRED: {len(self.__url__) * 6 * 3}s")
        
        # Iterate over the url's.
        for url in self.__url__:
            
            print(f"URL: {url}")
            self.__url_html__[url] = {}

            # Retrieve the HTML of the DYNAMIC page.
            for year in years:
                
                print(f"CURRENT YEAR: {year}")
                
                # Refers to January, 1st. Arbitrary decision.
                archive_url = 'https://web.archive.org/web/' + f"{year}" + '0101000000*/' + url
                self.__driver__.get(archive_url)    # Retrive the current HTML.
                print(f"SCRAPED {year}")
                print("zzz...zzz...zzz...")         # Let the scraper rest a bit...
                
                time.sleep(6)
                html = self.__driver__.page_source
                print(f"HTML STORED!")
                
                soup = BeautifulSoup(html, 'html.parser')       # Parse to get a neat structure.
                self.__url_html__[url].update({year : soup})
        
        return self.__url_html__
        
    
    def _get_snap_dates(self, soup: BeautifulSoup, scraping_year: str) -> list: 
        '''
        Given the HTML of the calendar from Wayback Machine as input, returns all the dates which represent a snapshot.

        Parameters
        ---
        soup : BeautifulSoup
            A `BeautifulSoup` object, containing a HTML.
        
        scraping_year : str
            The year in which the scraper should work.
        Outputs
        ---
        A list containing the dates of the snapshots for every year.
        '''

        self.__dates__ = []

        # Get the month class.
        css_selector = '[class="month"]'
        highlighted_elements = soup.select(css_selector)
        
        # Iterate over all the months.
        for month in range(0, 12):
            
            # Extract days of the month that contain a snapshot.
            for day in highlighted_elements[month].select('[style="touch-action: pan-y; user-select: none;"]'):
                
                self.__dates__.append("" + f"{scraping_year}/" + f"{str(month + 1)}/" + f"{str(day.get_text())}")

        # Transform into a np.array for efficiency reasons.
        self.__dates__ = np.array(self.__dates__)
        
        return self.__dates__


    def _dates_from_html(self, url_html: dict) -> dict:
        '''
        Extracts the dates within the `HTML` for which a snapshot is present.

        Parameters
        ---
        url_html : dict
            A `dict` containing both the `URL` and the related `HTML`.
        
        Output
        ---
        A `dict` containing the candidate dates within the `HTML`.
        '''

        self.__url_dates__ = {}

        # Iterate over the URLs.
        for url in list(url_html.keys()):
            
            soup = url_html[url]                # Store the HTML.
            self.__url_dates__[f"{url}"] = []   # Initialize the URL key.
            
            # Iterate over the years for a specific URL.
            for year in list(soup.keys()):
                
                self.__url_dates__[url].extend(self._get_snap_dates(soup[year], year))  # For each URL, append the year and the related dates.
        
        return self.__url_dates__
    

    def _date_from_url(url: str,  date_format: str = "%Y/%m/%d") -> datetime:
        '''
        Extract the date within the `URL`.

        Parameters
        ---
        url : str
            The URL that contains the date;
        
        date_format : str
            The format to consider when parsing the date.
        
        Output
        ---
        Date within the `URL`.
        '''

        pattern = r"http://mashable.com/(\d{4}/\d{2}/\d{2})/[-\w]+/"    # Set the RegEx pattern.
        match = re.search(pattern, url)                                 # Match the pattern.
        date = match.group(1)                                           # Extract the date.
        parsed_date = datetime.strptime(date, date_format).date()       # Parse the date to obtain a datetime.

        return parsed_date


    def _compute_shift_date(initial_date: str, timedelta_int: int) -> datetime:
        '''
        Takes as input a date and a number of days and computes the date after having summed the days.

        Parameters
        ---
        initial_date : str
            Starting date to which will be added `timedelta`;
        
        timedelta : int
            Shift in days to compute the new date.

        Output
        ---
        A new date, computed adding `timedelta` to `initial_date`.
        '''

        result_date = initial_date + timedelta(days = timedelta_int)    # Compute the new date

        return result_date


    def _get_closest(candidate_date: datetime, snap_dates: list, date_format: str = "%Y/%m/%d") -> datetime:
        '''
        Looks at a list of dates and returns the closest one to `candidate_date`. The dates in
        `real_dates` are the ones for which a snapshot of the website is available.

        Parameters
        ---
        candidate_date : datetime
            The threshold date, it should be the maximum date allowed to scrape.
        
        real_dates: list
            Dates for which snapshots of the website are available.
        
        Output
        ---
        The closest date to `candidate_date` in the past.
        '''

        masked = np.array(snap_dates)[np.array(snap_dates) <= candidate_date]                       # Filter possible candidate dates.
        closest = masked[(candidate_date - np.array([filtered for filtered in masked])).argmin()]   # Retrieve the closest date.

        return closest

    
    def get_closest(self, url: list, timedelta: list):
        '''
        Given a `list` of `timedelta` 
        '''

        date_new = map(self._date_from_url, self.__url__)                                               # When the news was posted.
        shifted_dates = map(self._compute_shift_date, date_new, timedelta)     # When the news was scraped.
        self._dates_from_html(self.__url_html__)

        for url, i in zip(self.__url_dates__, shifted_dates):
            snap = np.array([datetime.strptime(string, "%Y/%m/%d").date() for string in self.__url_dates__[url]])
            closest = self._get_closest(shifted_dates[i], snap)
        
        return closest
    
