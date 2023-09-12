import logging
import os
import shutil
from multiprocessing import Process, Queue, cpu_count
from typing import Any

from external.client import YandexWeatherAPI
from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import (CITIES, ReportExcelTable, create_new_folders,
                   excel_report_table_settings)

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

    completed_data_file_dir: str = 'analyses_done/'
    processes: list[Process] = [
        DataCalculationTask(
            input_queue,
            path=completed_data_file_dir
        ) for _ in range(cpu_count())
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join()

    logging.info('Вычисления средней температуры и осадков завершены.')
    shutil.rmtree('cities_analyses')

    logging.info('Начало анализа данных для расчета рейтинга.')
    rates: dict[str: float] = {}
    data_analysing_task = DataAnalyzingTask(
        file_dir=completed_data_file_dir,
        output_dict=rates,
    )
    data_analysing_task.rate_data()
    logging.info('Анализ данных для расчета рейтинга завершен.')

    logging.info('Начало формирования отчета в формате .xlsx')
    excel_file_path: str = 'results.xlsx'
    ReportExcelTable(
        file_path=excel_file_path,
        settings=excel_report_table_settings,
        records_amount=len(rates),
    ).create_and_setup_new_excel_file()

    data_aggregation_task = DataAggregationTask(
        file_dir=completed_data_file_dir,
        dict_with_rates=rates,
        report_path=excel_file_path,
    )
    answer: str = data_aggregation_task.aggregate_data()
    shutil.rmtree('analyses_done')
    # data_aggregation_task.write_report(excel_file_path)
    logging.info('Формирование отчета завершено.')

    # logging.info(
    #     'Формирование отчета завершено.'
    #     f'Наиболее благоприятные города для посещения: {answer}'
    # )


if __name__ == "__main__":
    forecast_weather()
