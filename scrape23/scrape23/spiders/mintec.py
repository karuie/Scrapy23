import scrapy
import json
from scrape23.items import IndexPriceItem
from datetime import datetime
from decimal import Decimal, DecimalException
import math
from dateutil.relativedelta import relativedelta
from environs import Env


SOURCE = 'mintec'
PATH_SEPARATOR = '\t'
current_date = datetime.now()
START_DATE = current_date - relativedelta(months=3)
END_DATE = current_date + relativedelta(months=3)
env = Env()
env.read_env('credential.env')
API_CLIENT_ID = env('Mintec_API_CLIENT_ID')
API_SECRET_KEY = env('Mintec_API_SECRET_KEY')



SERIES_CODES = ['MC62', 'MC61',
                'NAL0', 'CRS1', 'NAL1', 'NP01', 'RU23', 'RU24',
                '4D23', '4D24', '4D25', '1Y46', '4D22', 'RT19', 'RT21', 'RT36', 'RT37', 'RT38', 'RT39',
                '5U01', '5U02', '5U03', '5U04', '5U05', 'IT64', 'RU25', 'RU26',
                'XK27', '5U16',
                'BRM1', 'NBZ0', '4Q11', '4Q18', 'RU09',
                '1Y22', '1Y10', '1Y11', '1Y12', '1Y15', '1Y17', '4Q12', '1Y21', '1Z44', '1Z45', '1Z46', '1Z47', '4Q14',
                '0N101',
                '5U28', 'JE05', 'JE06',
                'TS53', '6Y15',
                'NDC3', 'NDC5', '3M07', '3M01', 'CD01', '5U19',
                '3M04', '3M02', 'DT08', 'DT12', 'DT13', '3M03', '7V02', '7V03',
                '1Y92', 'XK26', 'XK29', 'XK30',
                '4P01', '4D20', 'NHK6', 'NHK7', 'NNHK', '4D19', '4D21', 'NHK9', '5U06', '5U07', '5U08',
                'NS12', 'NS13', 'NS15',
                '1S01', '1S02', '1S03', '1S06', '1S07', 'PNU1', 'PNU2', 'PNU3', 'PNU4',
                'RZ42', 'RZ43', 'LG92', 'VL35', 'VL36', 'TS77', '1S08', '1S09', 'IU24', 'WB91',
                'BO01', 'BO02', 'BO04', 'BO05', '0C04', 'NPK2',
                'QS20', 'QS21', 'QS22', 'QS24', 'QS25', 'QS28', 'QS29', 'RU07', 'SD43', 'SD44',
                'NPC0', '5U09', '5U10',
                '5U22', '7R90',
                'DT17', 'DT18', 'DF01', 'DF02', 'DF04', '7R91', 'RU11', 'RU12', 'RU13', 'RU14', 'RU15',
                'SEMA', '4D15', '4D16', '1Y47', '4L48', '4M13', '5V15', 'SD29',
                'LG90', 'TS54', '5U17', '5U18', 'OF23',
                '5W47', '4D01', '4D10', '4D11', '4D60', '5U21',
                'XK28', '5U14', '5U15', '5V01',
                '8Z44', '8Z45', '8Z46', '4D12', '4D13', '4D14',
                '4D52', '4D53', '4D54', 'BO07', 'BO08', 'BO09', 'BO10', 'BO11', '4M16', '4M17',
                '3D20', 'SD32', 'SD34', 'LU56',
                'JE01', 'JE19', '5U13', '4D26', '4D27', '4D28', '4D29', '4D30', '4D31',
                '7R95', '7R96', '5V02', 'MSMH',
                '5U12', 'GR08', 'FL58', '1Y45', '7R86', '5U11', '5U29', '5U30', '1Y138', 'RU32']


class MintecSpider(scrapy.Spider):
    name = 'mintec'
    start_urls = ['https://www.mintecglobal.com/']

    def parse(self, response):
        yield scrapy.http.FormRequest(url='https://identity.mintecanalytics.com/connect/token',
                                      callback=self.parse_authorization,
                                      headers={'Content-Type': 'application/x-www-form-urlencoded'},
                                      formdata={'client_id': API_CLIENT_ID,
                                                'client_secret': API_SECRET_KEY,
                                                'grant_type': 'client_credentials',
                                                'scope': 'export_api'},
                                      method='POST')

    def parse_authorization(self, response):
        authorization = json.loads(response.body)
        access_token = authorization['access_token']
        token_type = authorization['token_type']
        for series_code in SERIES_CODES:
            series_url = f'https://public-api.mintecanalytics.com/v2/export/series/mintec/{series_code}/points'
            yield scrapy.http.FormRequest(url=series_url,
                                          callback=self.parse_series_code,
                                          headers={'Content-Type': 'application/x-www-form-urlencoded',
                                                   'Authorization': f'{token_type} {access_token}'},
                                          formdata={'startDate': START_DATE.strftime('%d/%m/%Y'),
                                                    'endDate': END_DATE.strftime('%d/%m/%Y'),
                                                    'gapFillOption': '0'},
                                          method='GET',
                                          cb_kwargs={'source_url': series_url})

    def parse_series_code(self, response, source_url):
        series_data = json.loads(response.body)
        series_code = series_data['content']['seriesCode']
        series_name = series_data['content']['seriesName']
        series_description = series_data['content']['seriesDescription']
        currency = series_data['content']['currencyName']
        units = series_data['content']['unitName']
        frequency = series_data['content']['frequencyName']
        country_of_origin = series_data['content']['countryOfOriginName']
        country_of_delivery = series_data['content']['countryOfDeliveryName']
        original_index_id = PATH_SEPARATOR.join([SOURCE, series_code, series_name, series_description,
                                                 currency, units, frequency, country_of_origin, country_of_delivery])
        index_specification = PATH_SEPARATOR.join([series_name, series_description, currency, units,
                                                   frequency, country_of_origin, country_of_delivery])
        data_points = series_data['content']['points']
        for point in data_points:
            date = point['date']
            price = point['value']

            try:
                date = datetime.strptime(date, '%d/%m/%Y')
            except (ValueError, TypeError) as ex:
                self.logger.error(f"Non date format row of {date} "
                                  f"found in {original_index_id}", exc_info=ex)

            try:
                price = str(price)
                price = Decimal(price)
                if not math.isnan(price):
                    print(price)
                    report_item = IndexPriceItem(
                        source=SOURCE,
                        source_url=source_url,
                        original_index_id=original_index_id,
                        index_specification=index_specification,
                        published_date=date,
                        price=price
                    )
                    yield report_item
            except DecimalException as ex:
                self.logger.error(f"Non decimal price of {price} "
                                  f"found in {original_index_id}", exc_info=ex)