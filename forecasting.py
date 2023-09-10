# import logging
# import threading
# import subprocess
# import multiprocessing
import os
from pprint import pprint
from typing import Any

from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, get_url_by_city_name


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """

    data_fetched_task: DataFetchingTask = DataFetchingTask(
        cities=CITIES,
        weather_api=YandexWeatherAPI,
    )
    fetched_data: tuple[tuple[str, dict[str, Any]]] = data_fetched_task.get_weather_data(
        max_workers=os.cpu_count() + 3
    )
    # print(type(fetched_data))




if __name__ == "__main__":
    forecast_weather()
