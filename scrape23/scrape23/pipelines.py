# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
import hashlib
from scrapy.utils.python import to_bytes
from urllib.parse import unquote
from scrapy.http import Request
import pandas as pd
# useful for handling different item types with a single interface
import os
from pathlib import Path
from pymongo import MongoClient
import requests
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter

from scrape23.items import IndexPriceItem

class SplitCSVExportPipeline:
    def open_spider(self, spider):
        self.opened_files = {}
        self.files = []

    def close_spider(self, spider):
        for filename, exporter in self.opened_files.items():
            exporter.finish_exporting()
        for i, f in enumerate(self.files):
            f.close()

    def _exporter_for_item(self, item):
        adapter = ItemAdapter(item)
        report_title = 'scrapes'
        report_section = adapter['source']
        report_directory = Path(f'./reports/{report_title}')
        report_directory.mkdir(parents=True, exist_ok=True)
        filename = report_directory / f"{report_section}.csv"
        if filename not in self.opened_files:
            filename.touch(exist_ok=True)
            f = filename.open(mode='wb')
            self.files.append(f)
            exporter = CsvItemExporter(f)
            exporter.start_exporting()
            self.opened_files[filename] = exporter
        return self.opened_files[filename]

    def process_item(self, item, spider):
        exporter = self._exporter_for_item(item)
        exporter.export_item(item)
        return item

