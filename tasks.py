import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue
from queue import Empty
from typing import Any

from utils import get_url_by_city_name


class DataFetchingTask:
    """
    Получение информации о погодных условиях для указанного списка городов.
    """

    def __init__(self, cities: dict[str, str], weather_api) -> None:
        """
        Инициализация объекта.
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
    def __init__(self, input_queue: Queue) -> None:
        """
        Инициализация объекта.
        Для работы с передаваемыми данными используется полученная очередь.
        """
        super().__init__()
        self.input_queue = input_queue

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
                f'analyses_done/{os.path.basename(file_path)}'
            ])


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass

