import logging
import os
import shutil
import sys
from multiprocessing import Process, Queue, cpu_count
from typing import Any

from external.client import YandexWeatherAPI
from tasks import (
    DataAggregationTask, DataAnalyzingTask,
    DataCalculationTask, DataFetchingTask)
from utils import (
    CITIES, ReportExcelTable, create_new_folders,
    excel_report_table_settings, internet_connection_is_available)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """
    if not internet_connection_is_available():
        logging.error('Internet connection is unavailable.')
        sys.exit(1)

    logging.info('Start collecting weather conditions data.')
    data_fetched_task = DataFetchingTask(
        cities=CITIES,
        weather_api=YandexWeatherAPI,
    )
    fetched_data: tuple[tuple[str, dict[str, Any] | None], ...] = (
        data_fetched_task.get_weather_data()
    )
    logging.info(
        'Weather data collection completed.'
        f'Number of loaded cities: {len(fetched_data)}.'
    )

    create_new_folders(('cities_analyses', 'analyses_done'))
    logging.info('Temporary directories for data files have been created.')

    logging.info('Start calculating average temperature and precipitation.')
    input_queue: Queue = Queue()
    for city in (data for data in fetched_data if data[1]):
        input_queue.put(city)

    completed_data_file_dir: str = os.path.join('analyses_done', '')
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

    logging.info(
        'Average temperature and precipitation calculations are complete.'
    )
    shutil.rmtree('cities_analyses')

    logging.info('Beginning of data analysis to calculate the rating.')
    rates: dict[str, tuple[float, float, int]] = {}
    data_analysing_task = DataAnalyzingTask(
        file_dir=completed_data_file_dir,
        output_dict=rates,
    )
    data_analysing_task.rate_data()
    logging.info('Data analysis to calculate the rating has been completed.')

    logging.info('Start generating a report in .xlsx format.')
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
    answer: list[str] = data_aggregation_task.aggregate_data()
    shutil.rmtree('analyses_done')
    logging.info('Temporary files deleted.')
    logging.info(
        'Report generation is complete. '
        f'The most favorable cities to visit: {", ".join(answer)}.'
    )


if __name__ == '__main__':
    forecast_weather()
