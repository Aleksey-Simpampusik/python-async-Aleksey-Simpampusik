import asyncio

import pandas as pd

from pandas.core.interchange.dataframe_protocol import DataFrame

from rarfile import RarFile


class ExcelParserAsync:
    """Парсер для работы с Excel файлами."""

    def __init__(self, file_name: str) -> None:
        """Инициализация парсера.

        Args:
            file_name: имя Excel файла.

        """

        self.__file_name = file_name
        self.__format_of_file = "xlsx"

    def set_file_name(self, new_file_name: str) -> None:
        """Установка нового имени файла.

        Args:
            new_file_name: Новое имя файла без расширения.

        """

        self.__file_name = new_file_name

    def get_file_name(self) -> str:
        """Получение текущего имени файла.

        Returns:
             Имя файла без расширения.

        """

        return self.__file_name

    def get_full_file_name(self) -> str:
        """Получение полного имени файла с расширением.

        Returns:
            Полное имя файла.

        """

        return f"{self.get_file_name()}.{self.__format_of_file}"

    async def excel_deserializer(self) -> DataFrame:
        """Десериализация Excel файла.

        Returns:
            Загруженные данные из Excel файла, либо None в случае ошибки загрузки.

        """

        loop = asyncio.get_running_loop()

        def _read_excel():
            opened_excel_file = None

            try:
                file = self.get_full_file_name()

                opened_excel_file = pd.read_excel(file)

                if file is None or len(file) == 0:
                    raise ValueError('DataFrame пустой или не загрузился')
            except FileNotFoundError:
                print("Такого файла нет.")
            except Exception as e:
                print(f"Произошла непредвиденная ошибка{e}")

            return opened_excel_file

        return await loop.run_in_executor(None, _read_excel)

    @staticmethod
    async def excel_serializer(
            list_devices: DataFrame,
            list_device_problems: DataFrame,
            list_of_calibration: DataFrame,
            device_pivot_table: DataFrame,
            output_file: str
    ) -> None:
        """Сохранение результатов анализа в Excel файл.

        Args:
            list_devices: DataFrame с исходным списком устройств.
            list_device_problems: DataFrame с проблемными клиниками.
            list_of_calibration: DataFrame с отчетом по срокам калибровки.
            device_pivot_table: DataFrame со сводной таблицей.
            output_file: имя выходного Excel файла.

        """

        loop = asyncio.get_running_loop()

        def _write_excel():
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                list_devices.to_excel(writer, sheet_name='Исходная таблица', index=False)

                if list_device_problems is not None:
                    list_device_problems.to_excel(writer, sheet_name='Проблемные клиники')

                if list_of_calibration is not None:
                    list_of_calibration.to_excel(writer, sheet_name='Отчет по срокам калибровки', index=False)

                if device_pivot_table is not None:
                    device_pivot_table.to_excel(writer, sheet_name='Сводная таблица')

        await loop.run_in_executor(None, _write_excel)


class RarFileParserAsync:
    """Парсер для работы с rar файлами."""

    def __init__(self, file_name: str) -> None:
        """Инициализация парсера.

        Args:
            file_name: имя Excel файла.

        """

        self.__file_name = file_name
        self.__format_of_file = "rar"

    def set_file_name(self, new_file_name: str) -> None:
        """Установка нового имени файла.

        Args:
            new_file_name: Новое имя файла без расширения.

        """

        self.__file_name = new_file_name

    def get_file_name(self) -> str:
        """Получение текущего имени файла.

        Returns:
             Имя файла без расширения.

        """

        return self.__file_name

    def get_full_file_name(self) -> str:
        """Получение полного имени файла с расширением.

        Returns:
            Полное имя файла.

        """

        return f"{self.get_file_name()}.{self.__format_of_file}"

    def rar_deserializer(self) -> list:
        """Десериализация Excel файла.

        Returns:
            Загруженные файлы из rar, либо None в случае ошибки загрузки.

        """

        full_file_name = self.get_full_file_name()

        files_names = None

        try:
            with RarFile(full_file_name) as my_rar:
                files_names = my_rar.namelist()

            if files_names is None or len(files_names) == 0:
                raise ValueError('DataFrame пустой или не загрузился')
        except FileNotFoundError:
            print("Такого файла нет.")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка{e}")

        return files_names
