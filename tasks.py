import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures._base import Future
from multiprocessing import Process, Queue
from queue import Empty
from statistics import mean
from typing import Any

import openpyxl

from utils import CITIES_NAMES_TRANSLATION, get_url_by_city_name


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
    ) -> tuple[tuple[str, dict[str, Any] | None], ...]:
        """
        Получение данных.
        """
        with ThreadPoolExecutor() as pool:
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
                self.output_dict[file_path] = (
                    avg_temp,
                    avg_cond_hours,
                    rating,
                )

    def count_rate_for_city(
        self,
        file_path: str,
    ) -> tuple[str, float, float, int]:
        """
        Добавление записи в словарь для расчета рейтинга.
        """
        with open(file_path, 'r') as file:
            days_data: list[dict[str, Any]] = json.load(file).get('days')
            common_temp_avg: float = round(
                mean(
                    day.get('temp_avg')
                    for day in days_data
                    if day.get('temp_avg')
                ),
                ndigits=1,
            )
            common_relevant_cond_hours: float = round(
                mean(
                    day.get('relevant_cond_hours')
                    for day in days_data
                    if day.get('relevant_cond_hours')
                ),
                ndigits=1,
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
    def __init__(
        self,
        file_dir: str,
        dict_with_rates: dict[str, tuple[float, float, int]],
        report_path: str,
    ) -> None:
        """
        Инициализация задачи для формирования отчета.
        """
        self.excel_file_path: str = report_path
        self.file_paths: list[str] = [
            os.path.join(file_dir, file) for file in os.listdir(file_dir)
        ]
        self.dict_with_rates: dict[str, tuple[float, float, int]] = (
            dict_with_rates
        )
        self.rating_indexes: list[int] = sorted(
            [
                rating
                for _, (_, _, rating)
                in self.dict_with_rates.items()
            ],
            reverse=True,
        )
        self.results_for_report: \
            list[tuple[tuple[float | str | Any | None, ...], ...]] = []
        self.answer: list[str] = []

    def aggregate_data(self) -> list[str]:
        """
        Агрегация и запись полученных данных в файл отчета.
        """
        with ThreadPoolExecutor() as pool:
            results: list[Future] = [
                pool.submit(self.get_data_tuple_for_city, file_path)
                for file_path
                in self.file_paths
            ]
            for future in as_completed(results):
                self.results_for_report.append(future.result())
        self.write_report(self.excel_file_path, self.results_for_report)
        return self.answer

    def get_data_tuple_for_city(
        self,
        file_path: str
    ) -> tuple[tuple[float | str | Any | None, ...], ...]:
        """
        Получение данных о погодных условиях для одного города.
        """
        with open(file_path, 'r') as file:
            file_data: list[dict[str, Any]] = json.load(file).get('days')
            avg_temp, avg_days, rating_coeff = self.dict_with_rates[
                file_path
            ]
            translated_name: str = CITIES_NAMES_TRANSLATION.get(
                city_name := file.name.removeprefix(
                    os.path.join('analyses_done', '')
                ).removesuffix('.json'),
                city_name,
            )
            rating_value: int = self.rating_indexes.index(rating_coeff)
            self.check_city_best_for_travel(rating_coeff, translated_name)
            return (
                (
                    translated_name,
                    'Температура, среднее',
                    *(day.get('temp_avg') for day in file_data),
                    avg_temp,
                    rating_value + 1,
                ),
                (
                    '',
                    'Без осадков, часов',
                    *(
                        day.get('relevant_cond_hours')
                        for day
                        in file_data
                    ),
                    avg_days,
                    '',
                ),
            )

    def check_city_best_for_travel(
        self,
        rating_coeff: int,
        translated_name: str,
    ) -> None:
        """
        Проверяет, является ли город благоприятным для посещения.
        """
        if rating_coeff == max(self.rating_indexes):
            self.answer.append(translated_name)

    @staticmethod
    def write_report(excel_file_path: str, results: list[Any]) -> None:
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
