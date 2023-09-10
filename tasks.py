from concurrent.futures import ThreadPoolExecutor
from utils import get_url_by_city_name
from typing import Any


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
            all_weather_data: set[tuple[str, dict[str, Any]]] = tuple(
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
        except Exception as error:
            # здесь сделать логгирование
            # print(error)
            return None
        return city, weather_data


class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
