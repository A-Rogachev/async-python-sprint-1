import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process, Queue
from queue import Empty
from statistics import mean
from typing import Any

import openpyxl

from utils import get_url_by_city_name, CITIES_NAMES_TRANSLATION


class DataFetchingTask:
    """
    Получение информации о погодных условиях для указанного списка городов.
    """

    def __init__(self, cities: dict[str, str], weather_api) -> None:
        """
        Инициализация объекта задачи для сбора информации о погодных условиях.
        """
        self.cities = cities
        self.weather_api = weather_api

    def get_weather_data(
        self,
        max_workers=None
    ) -> tuple[tuple[str, dict[str, Any] | None]]:
        """
        Получение данных.
        """
        with ThreadPoolExecutor(max_workers=None) as pool:
            all_weather_data = tuple(
                pool.map(
                    self.get_weather_data_for_one_city,
                    self.cities,
                )
            )
            return all_weather_data

    def get_weather_data_for_one_city(
        self,
        city: str
    ) -> tuple[str, dict[str, Any] | None]:
        """
        Получение информации о погодных условиях для одного города.
        """
        try:
            weather_data: dict[str, Any] = self.weather_api.get_forecasting(
                url=get_url_by_city_name(city)
            )
        except Exception:
            return city, None
        return city, weather_data


class DataCalculationTask(Process):
    """
    Вычисление средней температуры и анализ информации о осадках за указанный
    период для всех городов.
    """
    def __init__(self, input_queue: Queue, path: str) -> None:
        """
        Инициализация объекта.
        Для работы с передаваемыми данными используется полученная очередь.
        """
        super().__init__()
        self.input_queue = input_queue
        self.path = path

    def run(self):
        """
        Получает данные из очереди, с помощью вызова внешнего скрипта
        external/analyzer.py получает данные о погодных условиях и осадках.
        """
        while True:
            try:
                new_city = self.input_queue.get(timeout=1)
            except Empty:
                break

            city_name, city_data = new_city
            file_path = f'cities_analyses/{city_name}.json'
            with open(file_path, 'w') as file:
                json.dump(city_data, file)

            subprocess.run([
                'python',
                './external/analyzer.py',
                '-i',
                file_path,
                '-o',
                f'{self.path}{os.path.basename(file_path)}'
            ])


class DataAnalyzingTask:
    """
    Для анализа данных и выявления средней температуры и количества часов
    без осадков за полный период.
    """

    def __init__(self, file_dir, output_dict):
        """
        Инициализация задачи для расчета рейтинга города.
        """
        self.output_dict = output_dict
        self.file_paths = [
            os.path.join(file_dir, file) for file in os.listdir(file_dir)
        ]

    def rate_data(self):
        """
        В словарь добавляется запись с коэффициентом для города.
        """
        with ThreadPoolExecutor() as pool:
            results = [
                pool.submit(self.count_rate_for_city, file_path)
                for file_path
                in self.file_paths
            ]
            for future in as_completed(results):
                file_path, avg_temp, avg_cond_hours, rating = future.result()
                self.output_dict[file_path] = (avg_temp, avg_cond_hours, rating)

    def count_rate_for_city(self, file_path):
        """
        Добавление записи в словарь для расчета рейтинга.
        """
        with open(file_path, 'r') as file:
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
            return (
                file_path,
                common_temp_avg,
                common_relevant_cond_hours,
                round(common_temp_avg * common_relevant_cond_hours),
            )


class DataAggregationTask:
    """
    Формирование отчета о погодных условиях.
    """
    def __init__(self, file_dir, dict_with_rates, report_path):
        """
        Инициализация задачи для формирования отчета.
        """
        self.excel_file_path = report_path
        self.file_paths = [
            os.path.join(file_dir, file) for file in os.listdir(file_dir)
        ]
        self.dict_with_rates = dict_with_rates
        self.final_rating = [
            (city, rating) for city, (avg_temp, avg_cond_hours, rating) in self.dict_with_rates.items()
        ]
        self.rating_indexes = sorted(
            [
                rating 
                for _, (_, _, rating) 
                in self.dict_with_rates.items()
            ]
        )
        self.results = []

    def aggregate_data(self):
        """
        Агрегация данных для записи в отчет.
        """
        with ThreadPoolExecutor() as pool:
            results = [
                pool.submit(self.get_data_tuple_for_city, file_path)
                for file_path
                in self.file_paths
            ]
            for future in as_completed(results):
                self.results.append(future.result())
        self.write_report(self.excel_file_path, self.results)
    
    def get_data_tuple_for_city(self, file_path):
        """
        Получение данных о погодных условиях для одного города.
        """
        with open(file_path, 'r') as file:
            file_data = json.load(file).get('days')
            avg_temp, avg_days, rating_coeff = self.dict_with_rates[file_path]
            return (
                (
                    CITIES_NAMES_TRANSLATION.get(old_name := file.name.removeprefix('analyses_done/').removesuffix('.json'), old_name),
                    'Температура, среднее',
                    *(day.get('temp_avg') for day in file_data),
                    avg_temp,
                    self.rating_indexes.index(rating_coeff),
                ),
                (
                    '',
                    'Без осадков, часов',
                    *(day.get('relevant_cond_hours') for day in file_data),
                    avg_days,
                    '',
                ),
            )

    @staticmethod
    def write_report(excel_file_path, results):
        """
        Записывает построчно данные в отчет excel.
        """
        workbook = openpyxl.load_workbook(excel_file_path)
        sheet = workbook.active
        final_results = (
            result for data_result in results for result in data_result 
        )
        for row_num, data_tuple in enumerate(final_results, 2):
            for col_num, value in enumerate(data_tuple, 1):
                sheet.cell(row=row_num, column=col_num).value = value

        workbook.save(excel_file_path)
        workbook.close()

    #     # здесь задача 4
    #     ratings = sorted(rates.values(), key=lambda x: x)
    #     for key in rates:
    #         rates[key] = ratings.index(rates[key]) + 1
    #     # print(rates)

    #     for y in range(2, 32, 2):
    #         sheet[f'I{y}'] = rates[sheet[f'A{y}'].value]