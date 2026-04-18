import asyncio

from pandas.core.interchange.dataframe_protocol import DataFrame

import datetime as dt

import pandas as pd


class MainMenu:
    """Главное меню для анализа устройств."""

    def __init__(self, list_of_devices) -> None:
        """Конструктор класса.

        Args:
            list_of_devices: DataFrame с исходным списком устройств.

        """

        self.__list_of_devices = list_of_devices
        self.__now = dt.datetime.now()

    def get_list_of_devices(self) -> DataFrame:
        """Геттер списка девайсов.

        Returns:
            DataFrame с текущим списком устройств.

        """

        return self.__list_of_devices

    def set_list_of_devices(self, list_of_devices: DataFrame) -> None:
        """Сеттер списка девайсов.

        Args:
            list_of_devices: DataFrame с новым списком устройств.

        """

        self.__list_of_devices = list_of_devices

    def get_time_now(self) -> dt.datetime:
        """Геттер нынешнего времени.

        Returns:
            Текущая дата и время.

        """

        return self.__now

    async def normalize_time_async(self) -> None:
        """Нормализация временных столбцов в DataFrame."""

        list_of_devices = self.get_list_of_devices()

        columns = [
            "warranty_until", "install_date", "last_calibration_date", "last_service_date"
        ]

        try:
            tasks = [
                asyncio.to_thread(
                    pd.to_datetime,
                    list_of_devices[col],
                    format='%d.%m.%Y',
                    errors='coerce'
                )

                for col in columns
            ]

            results = await asyncio.gather(*tasks)

            for col, result in zip(columns, results):
                list_of_devices[col] = result

            self.set_list_of_devices(list_of_devices)

        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")

    def warranty_filter(self) -> None:
        """Фильтр устройств с действующей гарантией на текущую дату."""

        list_of_devices = self.get_list_of_devices()

        try:
            list_of_devices_warranty = list_of_devices[list_of_devices["warranty_until"] > self.get_time_now()]

            self.set_list_of_devices(list_of_devices_warranty)
        except ValueError:
            print("Ошибка типов данных\n")
        except Exception as e:
            print(f"произошла непредвиденная ошибка: {e}")

    def issues_sort(self) -> DataFrame:
        """Сортировка клиник по количеству проблем.

        Returns:
            DataFrame с проблемными клиниками, отсортированный по убыванию.

        """

        list_of_devices = self.get_list_of_devices()

        list_of_devices_problems = list_of_devices.groupby("clinic_id")[
            ['issues_reported_12mo', 'failure_count_12mo']].sum()

        list_of_devices_problems["sum_issues"] = list_of_devices_problems["issues_reported_12mo"] + \
                                                 list_of_devices_problems["failure_count_12mo"]

        list_of_devices_problems = list_of_devices_problems.sort_values(by=["sum_issues"], ascending=False)

        return list_of_devices_problems

    def calibration_report(self) -> DataFrame:
        """Формирование отчета по просроченной калибровке.

        Returns:
            DataFrame с количеством дней с калибровки.

        """

        list_of_devices = self.get_list_of_devices()

        calibration_list = list_of_devices[
            ((self.get_time_now() - list_of_devices["last_calibration_date"]).dt.days > 0)]

        calibration_list["Дней с последней калибровки"] = (
                    self.get_time_now() - calibration_list["last_calibration_date"]).dt.days

        return calibration_list

    def pivot_table_maker(self) -> DataFrame:
        """Создание сводной таблицы по клиникам и моделям оборудования.

        Returns:
            DataFrame со сводной таблицей: клиники vs модели оборудования.

        """

        list_of_devices = self.get_list_of_devices()

        pivot_count = list_of_devices.pivot_table(
            index='clinic_name',
            columns='model',
            values='device_id',
            aggfunc='count',
            fill_value=0
        )

        return pivot_count