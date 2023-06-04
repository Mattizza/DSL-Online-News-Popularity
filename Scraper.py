import requests
import re
import time
import numpy as np

from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta

import concurrent.futures


class Scraper():

    def __init__(self):
        '''
        Builds a `Scraper` object. Its goal is to make easier the scraping procedure
        and the management of the `URL`'s. 
        '''
        
        self = self


    def set_url(self, url: list) -> None:
        '''
        Pass a list `URL`'s. They will be stored within the object and will be considered
        through the scraping.

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


    def scrape(self, url: list = [], years: list = ['2013', '2014', '2015']) -> dict:
        '''
        Starts the scraping over the `years`. For each `URL`, It redirects the
        `Selenium` Driver to Calendar section in Wayback Machine. Then, collect
        the `HTML` and stores it. It will be of use when checking for the
        presence of dates for which a snapshot of the past is available.

        Parameters
        ---
        years : list, default = ['2013', '2014', '2015']
            List of the years over which the scraper will look.
        
        Output
        ---
        A dictionary containing the `URL` and the `HTML` associated for each year as follows:
        >>> {URL_0 : HTML_0,
        >>>  URL_1 : HTML_1
        >>>  ...
        >>> }
        '''
        
        if len(url) == 0:
            pool_url = self.__url__
        else:
            pool_url = url

        self.__url_html__ = {}      # Key = URL : Value = HTML
        print(f"YEARS: {years}")
        
        # Iterate over the url's.
        for url in pool_url:
            
            print(f"URL: {url}")
            self.__url_html__[url] = {}

            # Retrieve the HTML of the DYNAMIC page.
            for year in years:

                successful = False
                
                while not successful:
                    print(f"CURRENT YEAR: {year}")
                    # Refers to January, 1st. Arbitrary decision.
                    archive_url = 'https://web.archive.org/web/' + f"{year}" + '0101000000*/' + url
                    self.__driver__.get(archive_url)    # Retrive the current HTML.
                    print(f"SCRAPED {year}")
                    print("zzz...zzz...zzz...")         # Let the scraper rest a bit...
                    
                    time.sleep(2)
                    html = self.__driver__.page_source
                    print(f"HTML STORED!")
                    
                    soup = BeautifulSoup(html, 'html.parser')       # Parse to get a neat structure.
                    css_selector = '[class="month"]'
                    highlighted_month = soup.select(css_selector)
                    successful = len(highlighted_month) > 0

                self.__url_html__[url].update({year : soup})
        
        
        return self.__url_html__
        
    
    def parallelize_scrape(self, url_partition):


        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit the scraping tasks to the executor
            results = [executor.submit(self.scrape, url_fold) for url_fold in url_partition]
            # Retrieve the results as they complete
            for future in concurrent.futures.as_completed(results):
                scraped_data = future.result()
                # Process or store the scraped data as desired
        
        return scraped_data

    def get_snap_dates(self, list_html: dict = {}) -> dict:
        '''
        Given a list of `HTML`'s from the calendar section, returns
        all the dates for which a snapshot in the past is available.

        Parameters
        ---
        list_html : list
            List containing the `HTML`'s. Each `HTML` refers to a calendar page of Wayback Machine
            and stores the information about the snapshots and the related dates.
        
        Output
        ---
        A `dict` structured as follows:
        >>> {URL_0 : [snapshot_date_1, snapshot_date_1],
        >>>  URL_1 : [snapshot_date_1, snapshot_date_1],
        >>>  ...
        >>> }
        '''
        
        # If a list of URL's is not provided, refers to the one stored in the class.
        list_html = self.__url_html__ if not list_html else list_html
        dates = {}

        # Iterates over all the HTML.
        for html in list(list_html.keys()):
    
            soup = list_html[html]       # Store the HTML.
            dates[f"{html}"] = []       # Initialize the URL key.

            # Iterate over the years for a specific URL.
            for year in list(soup.keys()):

                dates[html].extend(self._get_snap_dates(soup[year], year))  # For each URL, append the year and the related dates.    

        self.__dates__ = dates

        return dates


    def _get_snap_dates(self, soup: BeautifulSoup, scraping_year: str) -> list: 
        '''
        Given the HTML of the calendar from Wayback Machine as input, returns all the dates which represent a snapshot.

        Parameters
        ---
        soup : BeautifulSoup
            A `BeautifulSoup` object, containing a HTML;
        
        scraping_year : str
            The year in which the scraper should work.
        Outputs
        ---
        A list containing the dates of the snapshots for every year.
        '''

        dates = []

        # Get the month class.
        css_selector = '[class="month"]'
        highlighted_elements = soup.select(css_selector)
        date_format = "%Y/%m/%d"

        # Iterate over all the months.
        for month in range(0, 12):
            
            # Extract days of the month that contain a snapshot.
            for day in highlighted_elements[month].select('[style="touch-action: pan-y; user-select: none;"]'):
                date_str = "" + f"{scraping_year}/" + f"{str(month + 1)}/" + f"{str(day.get_text())}"
                dates.append(datetime.strptime(date_str, date_format).date())

        # Transform into a np.array for efficiency reasons.
        dates = np.array(dates)
        
        return dates

    
    def shift_dates(self, initial_date_url: list, timedelta: list) -> list:
        '''
        Returns the initial date shifted by `timedelta` days in the future. `initial_date_url`
        must be the original `URL`.

        Parameters
        ---
        initial_date_url : list
            List containing the `URL`'s that contain the dates that will be shifted;
        
        timedelta : list
            List containing the days representing the shift in the future.
        
        Output
        ---
        List contaning the shifted dates.
        '''

        date_from_url = [self._date_from_url(i_url) for i_url in initial_date_url]
        self.__shifted_dates__ = [self._shift_date(date, timedel) for date, timedel in zip(date_from_url, timedelta)]

        return self.__shifted_dates__


    def _date_from_string(self, date_str_list: list, date_format = "%Y/%m/%d") -> datetime:
        '''
        Given a `str` date, returns the date in a `date.time` format.

        Parameters
        ---
        date_str_list : list
            List of dates in a `str` format;
        
        date_format : str, default = "%Y/%m/%d"
            Format of the date to be considered.
        
        Output
        ---
        Date in the `datetime` format.
        '''

        return np.array([datetime.strptime(string, date_format).date() for string in date_str_list])


    def get_closest(self, candidate_date: datetime, real_dates: list) -> datetime:
        '''
        Takes the date in which the data were collected and retrieve the closest date for which
        a snapshot is available.

        Parameters
        ---
        candidate_date : datetime
            The initial date shifted in the future;
        
        real_dates : list
            A list of `datetime` dates for which a snapshot of the past is available.
        
        Output
        ---
        Returns the closest `datetime` date for which a snapshot is available.
        '''

        masked = np.array(real_dates)[np.array(real_dates) <=candidate_date]   # Filter possible candidate dates.
        closest = masked[(candidate_date - np.array([filtered for filtered in masked])).argmin()]

        return closest


    def _dates_from_html(self, url_html: dict) -> dict:
        '''
        Extracts the dates within the `HTML` for which a snapshot is available.

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
            
            soup = url_html[url]            # Store the HTML.
            self.__url_dates__[f"{url}"] = []   # Initialize the URL key.
            
            # Iterate over the years for a specific URL.
            for year in list(soup.keys()):
                
                self.__url_dates__[url].extend(self._get_snap_dates(soup[year], year))  # For each URL, append the year and the related dates.
        
        return self.__url_dates__
    

    def _date_from_url(self, url: list,  date_format: str = "%Y/%m/%d", save = True) -> datetime:
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
        self.__parsed_date__ = parsed_date 

        return parsed_date


    def _shift_date(self, initial_date: str, timedelta_int: int) -> datetime:
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


    def _get_closest(self, candidate_date: datetime, snap_dates: list, date_format: str = "%Y/%m/%d") -> datetime:
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


    def switch_date(self, initial_url: str, switch_date: datetime) -> str:
        '''
        Starting from the initial 'URL', builds a new `URL` that redirects to the closest same webpage
        in the past.

        Parameters
        ---
        initial_url : str
            Starting `URL`;
        
        switch_date : datetime
            The date of the closest webpage in the past.
        
        Output
        ---
        The `URL` of the same webpage in another time.
        '''

        switched = {}
        for initial, switch in zip(initial_url, switch_date.keys()):
            
            switched.update({initial : "https://web.archive.org/web/" + str(switch_date[f"{switch}"]).replace("-", "") + "/" + initial})
            
        return switched


class ScrapePast(Scraper):


    def __init__(self):

        self = self


    def set_url(self, url: list) -> None:
        '''
        Pass a list `URL`'s. They will be stored within the object and will be considered
        through the scraping.

        Parameter
        ---
        url : list
            List of `URL`'s, each in a `str` format.
        '''
        
        super().set_url(url)


    def start_driver(self) -> None:
        '''
        Start a `Selenium` webdriver using Firefox as browser.
        '''

        super().start_driver()

    
    def recall_past(self, old_url: list):

        self.__old_url_html__ = {}
        self.__url_keywords__ = {}
        
        for url in old_url:

            time.sleep(1)
            html = requests.get(url)
            soup = BeautifulSoup(html.content, 'html.parser')

            meta_tag = soup.find('meta', attrs={'name': 'keywords', 'data-page-subject': 'true'})

            # Extract the content attribute value as a string
            keywords_string = meta_tag['content']

            # Split the keywords into a list
            keywords_list = keywords_string.split(', ')
            
            self.__old_url_html__[f"{url}"] = soup
            self.__url_keywords__[f"{url}"] = keywords_list
        
        return self.__old_url_html__, self.__url_keywords__


    # def scrape(self) -> dict:
    #     '''
    #     Starts the scraping over the `years`. For each `URL`, It redirects the
    #     `Selenium` Driver to Calendar section in Wayback Machine. Then, collect
    #     the `HTML` and stores it. It will be of use when checking for the
    #     presence of dates for which a snapshot of the past is available.

    #     Parameters
    #     ---
    #     years : list, default = ['2013', '2014', '2015']
    #         List of the years over which the scraper will look.
        
    #     Output
    #     ---
    #     A dictionary containing the `URL` and the `HTML` associated for each year as follows:
    #     >>> {URL_0 : HTML_0,
    #     >>>  URL_1 : HTML_1
    #     >>>  ...
    #     >>> }
    #     '''
        
    #     self.__url_html__ = {}      # Key = URL : Value = HTML
    #     print(f"URL: {len(self.__url__)}")
    #     print(f"\nSTART SCRAPING -- EXPECTED TIME REQUIRED: {len(self.__url__) * 6 * 3}s")
    #     self.__url_keywords__ = {}

    #     # Iterate over the url's.
    #     for url in self.__url__:
            
    #         successful = False

    #         while not successful:
                
    #             print(f"URL: {url}")
    #             self.__url_html__[url] = {}

    #             # Retrieve the HTML of the DYNAMIC page.
        
    #             self.__driver__.get(url)    # Retrive the current HTML.
    #             print("zzz...zzz...zzz...")         # Let the scraper rest a bit...
    #             time.sleep(2)
    #             html = self.__driver__.page_source
    #             print(f"HTML STORED!")
                
    #             soup = BeautifulSoup(html, 'html.parser')       # Parse to get a neat structure.
                
    #             meta_tag = soup.find('meta', attrs={'name': 'keywords', 'data-page-subject': 'true'})

    #             # Extract the content attribute value as a string
    #             keywords_string = meta_tag['content']

    #             # Split the keywords into a list
    #             keywords_list = keywords_string.split(', ')
                
    #             self.__url_keywords__[f"{url}"] = keywords_list

    #             successful = len(keywords_list) > 0

    #         self.__url_html__[url] = soup
        
    #     return self.__url_html__, self.__url_keywords__


