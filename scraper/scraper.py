import numpy as np
import pandas as pd
import re
import datetime
from scrapy import Selector
from requests import get
from math import ceil
from glob import glob
from os import path, chdir, mkdir, getcwd
from tkinter import messagebox

from util import log


class ScraperOLX:
    """Class that scrapes the prices of all apartments in Tashkent."""
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
            "Accept-Encoding": "*",
            "Connection": "keep-alive"
        }
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        self.today = today.strftime("%d-%m-%Y")
        self.yesterday = yesterday.strftime("%d-%m-%Y")

        # getting the exchange rate from www.cbu.uz
        fx_rate_url = 'https://cbu.uz/oz/'
        fx_rate_html = get(fx_rate_url, headers=self.headers).content
        fx_rate_selector = Selector(text=fx_rate_html)
        fx_rate_xpath = '//div[@class="exchange__content"]//div[@class="exchange__item_value"]'
        fx_rates = fx_rate_selector.xpath(fx_rate_xpath)
        fx_rates = fx_rates.xpath('./text()').extract()
        self.usd_to_uzs = float(fx_rates[0].replace(" = ", ""))
        self.district_dict = {
            20: 'Olmazor', 18: 'Bektemir', 13: 'Mirobod', 12: 'Mirzo-Ulugbek',
            19: 'Sergeli', 21: 'Uchtepa', 23: 'Chilonzor', 24: 'Shayhontohur',
            25: 'Yunusobod', 26: 'Yakkasaroy', 22: 'Yashnobod'
        }
        self.month_dict = {
            " г.": "", ' января ': '-01-', ' февраля ': '-02-', ' марта ': '-03-',
            ' апреля ': '-04-', ' мая ': '-05-', ' июня ': '-06-',
            ' июля ': '-07-', ' августа ': '-08-', ' сентября ': '-09-',
            ' октября ': '-10-', ' ноября ': '-11-', ' декабря ': '-12-',
            re.compile("^Сегодня.*"): self.today, re.compile("^Вчера.*"): self.yesterday
        }

    def scrape_announcement(self, dataframe, url):
        """
        Scrape an individual OLX announcement given its url.

        Adds a row to the end the dataframe that is passed as an argument. That
        row will contain information gathered from the announcement.

        Parameters
        ----------
        dataframe : pandas DataFrame
            Dataframe whose rows contain information gathered from the
            current and previous announcements.
        url : str
            url of the OLX announcement to be scraped.

        Returns
        -------
        None.

        """
        html = get(url, headers=self.headers).content
        html_selector = Selector(text=html)
        row = dataframe.shape[0]
        dataframe.at[row, 'link'] = url

        try:
            district_list = html_selector.xpath('//*[@id="root"]//a/text()').extract()
            district_pattern = re.compile(r"Продажа - (.*) район")
            district = list(filter(district_pattern.match, district_list))[0]
            district = re.sub(district_pattern, r"\1", district)
            dataframe.at[row, 'district'] = district
        except:
            pass

        try:
            date = html_selector.xpath('//*[@id="root"]/div[1]/div[3]/div[2]/div[1]/div[2]/div[1]/span/span')
            date = date.xpath(".//text()").extract_first()
            dataframe.at[row, 'date'] = date
        except:
            pass

        try:
            price_list = html_selector.xpath(
                '//*[@id="root"]/div[1]/div[3]/div[2]/div[1]/div[2]/div[3]/h3')
            price_list = price_list.xpath('.//text()').extract()
            price = float(price_list[0].replace(" ", ""))
            if price_list[-1] == 'сум':
                price = price / self.usd_to_uzs
            elif price_list[-1] != 'у.е.':
                price = np.nan
            dataframe.at[row, 'price'] = price
        except:
            pass

        # Other details
        try:
            other_details = html_selector.xpath('//*[@id="root"]/div[1]/div[3]/div[2]/div[1]/div[2]/ul/li/p')
            other_details = other_details.xpath('.//text()').extract()
        except:
            other_details = ""

        try:
            home_type_pattern = re.compile(r"Тип жилья: (.*)")
            home_type = list(filter(home_type_pattern.match, other_details))[0]
            home_type = re.sub(home_type_pattern, r"\1", home_type)
            dataframe.at[row, 'home_type'] = home_type
        except:
            pass

        try:
            rooms_pattern = re.compile(r"Количество комнат: (\d+).*")
            rooms = list(filter(rooms_pattern.match, other_details))[0]
            rooms = re.sub(rooms_pattern, r"\1", rooms)
            rooms = int(rooms)
            dataframe.at[row, 'num_rooms'] = rooms
        except:
            pass

        try:
            area_pattern = re.compile(r"Общая площадь: (\d+).*")
            area = list(filter(area_pattern.match, other_details))[0]
            area = re.sub(area_pattern, r"\1", area)
            area = int(area)
            dataframe.at[row, 'area'] = area
        except:
            pass

        try:
            dataframe.at[row, 'price_m2'] = dataframe.at[row, 'price'] / dataframe.at[row, 'area']
        except:
            pass

        try:
            floor_pattern = re.compile(r"Этаж: (\d+).*")
            floor = list(filter(floor_pattern.match, other_details))[0]
            floor = re.sub(floor_pattern, r"\1", floor)
            floor = int(floor)
            dataframe.at[row, 'apart_floor'] = floor
        except:
            pass

        try:
            home_floor_pattern = re.compile(r"Этажность дома: (\d+).*")
            home_floor = list(filter(home_floor_pattern.match, other_details))[0]
            home_floor = re.sub(home_floor_pattern, r"\1", home_floor)
            home_floor = int(home_floor)
            dataframe.at[row, 'home_floor'] = home_floor
        except:
            pass

        try:
            building_type_pattern = re.compile(r"Тип строения: (.*)")
            building_type = list(filter(building_type_pattern.match, other_details))[0]
            building_type = re.sub(building_type_pattern, r"\1", building_type)
            dataframe.at[row, 'build_type'] = building_type
        except:
            pass

        try:
            plan_pattern = re.compile(r"Планировка: (.*)")
            plan = list(filter(plan_pattern.match, other_details))[0]
            plan = re.sub(plan_pattern, r"\1", plan)
            dataframe.at[row, 'build_plan'] = plan
        except:
            pass

        try:
            year_pattern = re.compile(r"Год постройки.*(\d{4})")
            year = list(filter(year_pattern.match, other_details))[0]
            year = re.sub(year_pattern, r"\1", year)
            year = int(year)
            dataframe.at[row, 'build_year'] = year
        except:
            pass

        try:
            bath_type_pattern = re.compile(r"Санузел: (.*)")
            bath_type = list(filter(bath_type_pattern.match, other_details))[0]
            bath_type = re.sub(bath_type_pattern, r"\1", bath_type)
            dataframe.at[row, 'bathroom'] = bath_type
        except:
            pass

        try:
            furnished_pattern = re.compile(r"Меблирована: (.*)")
            furnished = list(filter(furnished_pattern.match, other_details))[0]
            furnished = re.sub(furnished_pattern, r"\1", furnished)
            dataframe.at[row, 'furnished'] = {'Да': True, 'Нет': False}.get(furnished)
        except:
            pass

        try:
            height_pattern = re.compile(r"Высота потолков: (.*)")
            height = list(filter(height_pattern.match, other_details))[0]
            height = re.sub(height_pattern, r"\1", height)
            height = float(height)
            if height > 150:
                height /= 100
            elif height >= 20:
                height /= 10

            dataframe.at[row, 'ceil_height'] = height
        except:
            pass

        try:
            condition_pattern = re.compile(r"Ремонт: (.*)")
            condition = list(filter(condition_pattern.match, other_details))[0]
            condition = re.sub(condition_pattern, r"\1", condition)
            dataframe.at[row, 'condition'] = condition
        except:
            pass

        try:
            commission_pattern = re.compile(r"Комиссионные: (.*)")
            commission = list(filter(commission_pattern.match, other_details))[0]
            commission = re.sub(commission_pattern, r"\1", commission)
            dataframe.at[row, 'commission'] = {'Да': True, 'Нет': False}.get(commission)
        except:
            pass

        # Title and text parts
        try:
            title = html_selector.xpath('//*[@id="root"]/div[1]/div[3]/div[2]/div[1]/div[2]/div[2]/h1')
            title = title.xpath('.//text()').extract_first()
            dataframe.at[row, 'title_text'] = title
        except:
            pass

        try:
            content = html_selector.xpath('//*[@id="root"]/div[1]/div[3]/div[2]/div[1]/div[2]/div[8]/div')
            content = content.xpath('.//text()').extract_first()
            dataframe.at[row, 'post_text'] = content
        except:
            pass

        # Extra Details
        try:
            close_things_pattern = re.compile(r"Рядом есть:")
            close_things = list(filter(close_things_pattern.match, other_details))[0]
        except:
            close_things = ""

        if 'Больница' in close_things:
            dataframe.at[row, 'hospital'] = True
        else:
            dataframe.at[row, 'hospital'] = False

        if 'Детская площадка' in close_things:
            dataframe.at[row, 'playground'] = True
        else:
            dataframe.at[row, 'playground'] = False

        if 'Детский сад' in close_things:
            dataframe.at[row, 'kindergarten'] = True
        else:
            dataframe.at[row, 'kindergarten'] = False

        if 'Парк' in close_things:
            dataframe.at[row, 'park'] = True
        else:
            dataframe.at[row, 'park'] = False

        if 'Развлекательные заведения' in close_things:
            dataframe.at[row, 'recreation'] = True
        else:
            dataframe.at[row, 'recreation'] = False

        if 'Рестораны' in close_things:
            dataframe.at[row, 'restaurant'] = True
        else:
            dataframe.at[row, 'restaurant'] = False

        if 'Школа' in close_things:
            dataframe.at[row, 'school'] = True
        else:
            dataframe.at[row, 'school'] = False

        if 'Супермаркет' in close_things:
            dataframe.at[row, 'supermarket'] = True
        else:
            dataframe.at[row, 'supermarket'] = False

    def scrape_page(self, df, section_url, page):
        """
        Scrape an OLX page that usually contains 39 individual announcements.
        If the page is the last page of the section, it may contain fewer than
        39 announcements.

        Modifies the dataframe that is passed as an argument.

        Parameters
        ----------
        df : pandas DataFrame
            Dataframe whose rows will contain information gathered from
            individual announcements.
        section_url : str
            url of the OLX section to be scraped.
        page : int
            The page number of the section to be scraped. This number is
            shown at the bottom of a web-page when necessary filters are
            applied to find an apartment/house.

        Returns
        -------
        None.

        """
        page_url = section_url + f'&page={page}'
        html = get(page_url, headers=self.headers).content
        html_selector = Selector(text=html)
        ad_links_xpath = '//*[@id="offers_table"]//a[@class="marginright5 link linkWithHash detailsLink"]'
        ad_links = html_selector.xpath(ad_links_xpath)
        ad_links = ad_links.xpath("./@href").extract()
        for advertisement in ad_links:
            try:
                self.scrape_announcement(df, advertisement)
            except:
                pass

    def scrape_section(self, df, commission, furnished, home_type, district_code):
        """
        Scrape all announcements of an OLX section with the given parameters.

        Modifies the dataframe that is passed as an argument.

        Parameters
        ----------
        df : pandas DataFrame
            Dataframe whose rows will contain information gathered from
            individual announcements.
        commission : str
            `yes` or `no` depending on if a broker commission is paid upon purchase.
        furnished : str
            `yes` or `no` depending on if a house is furnished.
        home_type : str
            It is either `novostroyki` or `vtorichnyy-rynok`.
        district_code : int
            Code of the district. Refer to `district_dict` to find a code for
            the district needed.

        Returns
        -------
        None.

        """
        section_url = f'https://www.olx.uz/nedvizhimost/kvartiry/prodazha/{home_type}'\
            f'/tashkent/?search%5Bfilter_enum_furnished%5D%5B0%5D={furnished}&search'\
            f'%5Bfilter_enum_comission%5D%5B0%5D={commission}&search%5B'\
            f'district_id%5D={district_code}'
        html = get(section_url, headers=self.headers).content
        html_selector = Selector(text=html)
        num_ads_xpath = '//*[@id="offers_table"]//div[@class="dontHasPromoted section clr rel"]/h2'
        number_ads = html_selector.xpath(num_ads_xpath)
        number_ads = number_ads.xpath('.//text()').extract_first()
        number_ads = re.findall(r'[0-9]+\s?[0-9]*', number_ads)
        number_ads = number_ads[0].replace(" ", "")
        number_ads = int(number_ads)
        num_pages = ceil(number_ads / 39)
        num_pages = min(num_pages, 2)  # TODO
        for page in range(1, num_pages + 1):
            try:
                self.scrape_page(df, section_url, page)
            except:
                pass

    def scrape_everything(self):
        """
        Scrape all announcements on OLX.uz that are about the sale of
        apartments in Tashkent.

        Returns
        -------
        None.

        """
        if not path.isdir("temporary_files"):
            mkdir("temporary_files")

        chdir("temporary_files")
        column_names = ['link', 'date', 'price', 'home_type', 'district', 'price_m2',
                        'furnished', 'commission', 'num_rooms', 'area', 'apart_floor',
                        'home_floor', 'condition', 'build_type', 'build_plan',
                        'build_year', 'bathroom', 'ceil_height', 'hospital',
                        'playground', 'kindergarten', 'park', 'recreation', 'school',
                        'restaurant', 'supermarket', 'title_text', 'post_text']
        commission_list = ['yes', 'no']
        furnished_list = ['yes', 'no']
        home_type_list = ['novostroyki', 'vtorichnyy-rynok']
        district_code_list = [20, 18, 13, 12, 19, 21, 23, 24, 25, 26, 22]
        for commission in commission_list:
            for furnished in furnished_list:
                for home_type in home_type_list:
                    for district_code in district_code_list:
                        log("info", f'Analyzing commission={commission}, furnished={furnished},'
                              f' home_type={home_type} for {self.district_dict[district_code]}')
                        filename_pkl = f'{self.today}-{commission}-{furnished}-{home_type}-'\
                            f'{self.district_dict[district_code]}.pkl'
                        if path.isfile(filename_pkl):
                            log("info", filename_pkl, "already exists.")
                            continue
                        else:
                            df = pd.DataFrame(columns=column_names)
                            try:
                                self.scrape_section(df, commission, furnished, home_type, district_code)
                                df.loc[:, 'date'] = df.loc[:, 'date'].replace(self.month_dict, regex=True)
                                df.dropna(how="all", inplace=True,
                                          subset=["price", 'num_rooms', 'area', 'apart_floor'])
                                df.to_pickle(filename_pkl)
                                log("info", f'Number of observations scraped: {len(df)}.')
                            except:
                                pass
        chdir("..")

        # df.drop_duplicates(subset=column_names, inplace=True)
        # df.to_excel(database_name_excel, index=False, encoding="utf-8")

    def create_excel(self):
        if not path.isdir("Database"):
            self.create_messagebox('Database folder does not exist.')
            return

        chdir("Database")
        filename = f'{self.today} merged.pkl.zip'  # TODO
        if not path.isfile(filename):
            self.create_messagebox(f'{filename} does not exist.')
            chdir("..")
            return

        dataframe = pd.read_pickle(filename)
        filename_new = f'{self.today} merged.xlsx'
        dataframe.to_excel(filename_new, index=False, encoding="utf-8")
        self.create_messagebox(f'{filename_new} has been created.', False)
        chdir("..")

    def merge_district_pickles(self):
        if not path.isdir("temporary_files"):
            self.create_messagebox('temporary_files folder does not exist.')
            return            

        chdir("temporary_files")
        filenames_pattern = f'{self.today}-*pkl'
        all_filenames = [file for file in glob(filenames_pattern)]
        if not all_filenames:
            self.create_messagebox(f'Pickle files do not exist in {getcwd()}')
            chdir("..")
            return

        print(f'Found {len(all_filenames)} files to merge.')
        merged_data = pd.concat([pd.read_pickle(file) for file in all_filenames])
        chdir("..")

        if not path.isdir("Database"):
            mkdir("Database")
        chdir("Database")
        merged_filename = f'{self.today} merged.pkl.zip'
        merged_data.to_pickle(merged_filename)
        self.create_messagebox(f'{merged_filename} has been created.', False)
        chdir("..")
        
    def merge_month_pickles(self):
        if not path.isdir("Database"):
            self.create_messagebox('Database folder does not exist.')
            return

        chdir("Database")
        filenames_pattern = "Hey"  # TODO
        

        chdir("..")
        

        
        

    def create_messagebox(self, text, is_error=True):
        if is_error:
            messagebox.showerror(title="Saving Request", message=text)
        else:
            messagebox.showinfo(title="Saving Request", message=text)
