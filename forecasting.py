import json
import logging
import os
from multiprocessing import Process, Queue, cpu_count
from pprint import pprint
from statistics import mean
from typing import Any

import openpyxl
# from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from external.client import YandexWeatherAPI
from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, CITIES_NAMES_TRANSLATION, create_new_folders, excel_report_table_settings, ReportExcelTable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """

    logging.info('Начало сбора данных о погодных условиях.')
    data_fetched_task = DataFetchingTask(
        cities=CITIES,
        weather_api=YandexWeatherAPI,
    )
    fetched_data: tuple[tuple[str, dict[str, Any]]] = (
        data_fetched_task.get_weather_data(max_workers=os.cpu_count() + 3)
    )
    logging.info(
        'Сбор данных о погодных условиях завершен.'
        f'Количество загруженных городов: {len(fetched_data)}.'
    )

    create_new_folders('cities_analyses', 'analyses_done')
    logging.info('Созданы временные директории для файлов с данными.')

    logging.info('Начало вычисления средней температуры и осадков.')
    input_queue: Queue = Queue()
    for city in (data for data in fetched_data if data[1]):
        input_queue.put(city)

    processes: list[Process] = [
        DataCalculationTask(input_queue) for _ in range(cpu_count())
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join()

    logging.info('Вычисления средней температуры и осадков завершены.')
    logging.info('Начало агрегации данных.')

    excel_report_table = ReportExcelTable(
        file_path='results.xlsx',
        settings=excel_report_table_settings,
    )

    rates = {}
    for file in os.listdir('analyses_done/'):
        with open(f'analyses_done/{file}', 'r') as file:
            days_data = json.load(file).get('days')
            common_temp_avg: float = round(
                mean(
                    day.get('temp_avg')
                    for day in days_data
                    if day and day.get('temp_avg')
                ), 1
            )
            common_relevant_cond_hours: float = round(
                mean(
                    day.get('relevant_cond_hours')
                    for day in days_data
                    if day and day.get('relevant_cond_hours')
                ), 1
            )
            rates[file.name.removeprefix('analyses_done/').removesuffix('.json')] = (
                common_temp_avg * common_relevant_cond_hours
            )
    print(rates)

    # with open(excel_report_table, 'w') as excel_file:
    #     for i, file in enumerate(os.listdir('analyses_done')):
    #         with open(f'analyses_done/{file}', 'r') as file:
    #             data = json.load(file)
    #             name = file.name.removeprefix('analyses_done/').removesuffix('.json')
    #             name = CITIES_NAMES_TRANSLATION[name]
    #             sheet[f'A{i + 2 + i}'] = name
    #             sheet[f'B{i + 2 + i}'] = 'Температура, среднее'
    #             sheet[f'B{i + 3 + i}'] = 'Без осадков, часов'
    #             days_data = data.get('days')

    #             columns = ['C', 'D', 'E', 'F', 'G']
    #             for j, day_data in enumerate(days_data):
    #                 sheet[f'{columns[j]}{i + 2 + i}'] = day_data.get('temp_avg')
    #                 sheet[f'{columns[j]}{i + 3 + i}'] = day_data.get('relevant_cond_hours')
    #                 sheet[f'{columns[j]}{i + 2 + i}'].alignment = excel_report_table_settings.get('center_alignment')
    #                 sheet[f'{columns[j]}{i + 3 + i}'].alignment = excel_report_table_settings.get('center_alignment')
    #             sheet[f'H{i + 2 + i}'] = round(mean(data.get('temp_avg') for data in days_data if data.get('temp_avg')), 1)
    #             sheet[f'H{i + 3 + i}'] = round(mean(data.get('relevant_cond_hours') for data in days_data if data.get('relevant_cond_hours')), 1)
    #             rates[name] = sheet[f'H{i + 2 + i}'].value * sheet[f'H{i + 3 + i}'].value
    #         i += 1

    #     # здесь задача 4
    #     ratings = sorted(rates.values(), key=lambda x: x)
    #     for key in rates:
    #         rates[key] = ratings.index(rates[key]) + 1
    #     # print(rates)

    #     for y in range(2, 32, 2):
    #         sheet[f'I{y}'] = rates[sheet[f'A{y}'].value]

    # ##############################################################################

    #     for k in range(1, 32):
    #         for cell in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'):
    #             sheet[f'{cell}{k}'].border = excel_report_table_settings.get('thin_border')

    #     for m in range(3, 32, 2):
    #         for cell in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'):
    #             sheet[f'{cell}{m}'].fill = excel_report_table_settings.get('color_fill')

    #     wb.save('results.xlsx')
    #     wb.close()

if __name__ == "__main__":
    forecast_weather()
