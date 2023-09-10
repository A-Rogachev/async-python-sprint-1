# import logging
# import threading
import shutil
import subprocess
import multiprocessing
import os
import json
from pprint import pprint
from typing import Any
from openpyxl.styles import Font
import openpyxl
from utils import CITIES_NAMES_TRANSLATION

from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, get_url_by_city_name, create_new_folder


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """

    data_fetched_task = DataFetchingTask(
        cities=CITIES,
        weather_api=YandexWeatherAPI,
    )
    fetched_data = (
        data_fetched_task.get_weather_data(max_workers=os.cpu_count() + 3)
    )
    fetched_data = [data for data in fetched_data if data[1]]

    # Задача № 2
    create_new_folder('cities_analyses')
    create_new_folder('analyses_done')

    for city in fetched_data:
        with open(f'cities_analyses/{city[0]}.json', 'w') as file:
            json.dump(city[1], file)

    for file in os.listdir('cities_analyses'):
        subprocess.run(
            [
                'python',
                './external/analyzer.py',
                '-i',
                f'cities_analyses/{file}',
                '-o',
                f'analyses_done/{file}'
            ]
        )

    # Задача № 3
    wb = openpyxl.Workbook()
    wb.active.title = 'Анализ погоды'
    sheet = wb['Анализ погоды']
    bold_font = Font(bold=True)

    sheet_names = {
        'A1': 'Город/день',
        'C1': '26-05',
        'D1': '27-05',
        'E1': '28-05',
        'F1': '29-05',
        'G1': '30-05',
        'H1': 'Среднее',
        'I1': 'Рейтинг',
    }
    
    for key, value in sheet_names.items():
        sheet[key] = value
        sheet[key].font = bold_font
        # sheet[key].font = font
                
    

    for i, file in enumerate(os.listdir('analyses_done')):
        with open(f'analyses_done/{file}', 'r') as file:
            data = json.load(file)
            name = file.name.removeprefix('analyses_done/').removesuffix('.json')
            sheet[f'A{i + 2}'] = CITIES_NAMES_TRANSLATION[name]
            days_data = data.get('days')
            # pprint(data)

            columns = ['C', 'D', 'E', 'F', 'G']
            for j, day_data in enumerate(days_data):
                print(day_data)
                sheet[f'{columns[j]}{i + 2}'] = day_data.get('temp_avg')
                
                
                

    wb.save('results.xlsx')
    wb.close()    

if __name__ == "__main__":
    forecast_weather()
