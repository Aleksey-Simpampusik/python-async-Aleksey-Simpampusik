import asyncio

from menu import(
    MainMenu as Menu,
)
from menu_async import (
    MainMenu as MenuAsync,
)

from parsers import (
    RarFileParser, ExcelParser,
)

from parsers_async import (
    ExcelParserAsync, RarFileParserAsync,
)

import time


async def process_single_file(rar_file: str, base_path: str) -> None:
    """Обработка одного RAR файла."""
    excel_parser = ExcelParserAsync(f"{base_path}/{rar_file[:-5]}")

    list_devices = await excel_parser.excel_deserializer()

    if list_devices is None:
        print(f"Пропуск {rar_file}: не удалось прочитать данные")
        return

    menu = MenuAsync(list_devices)

    await menu.normalize_time_async()
    menu.warranty_filter()
    list_devices_problems = menu.issues_sort()
    list_of_calibration = menu.calibration_report()
    device_pivot_table = menu.pivot_table_maker()

    await excel_parser.excel_serializer(
        menu.get_list_of_devices(),
        list_devices_problems,
        list_of_calibration,
        device_pivot_table,
        rar_file
    )


async def main_async() -> None:
    """Запуск всех заданий анализа асинхронно и параллельно."""

    base_path = "/Users/simpampusik/PycharmProjects/python-async-Aleksey-Simpampusik/async_data"
    rar_parser = RarFileParserAsync("async_data")

    rar_files_names = rar_parser.rar_deserializer()

    tasks = [process_single_file(rar_file, base_path) for rar_file in rar_files_names]
    await asyncio.gather(*tasks)


async def main_sync() -> None:
    """Запуск всех заданий анализа синхронно."""

    rar_parser = RarFileParser("async_data")

    rar_files_names = rar_parser.rar_deserializer()

    for rar_file in rar_files_names:
        excel_parser = ExcelParser(f"/Users/simpampusik/PycharmProjects/python-async-Aleksey-Simpampusik/async_data/{rar_file[:-5]}")

        list_devices = excel_parser.excel_deserializer()

        menu = Menu(list_devices)

        menu.normalize_time()

        menu.warranty_filter()

        list_devices_problems = menu.issues_sort()

        list_of_calibration = menu.calibration_report()

        device_pivot_table = menu.pivot_table_maker()

        excel_parser.excel_serializer(
            menu.get_list_of_devices(),
            list_devices_problems,
            list_of_calibration,
            device_pivot_table,
            rar_file
        )


if __name__ == "__main__":
    start_sync = time.time()

    asyncio.run(main_sync())

    end_sync = time.time()

    print(f"Синхронное выполнение: {end_sync - start_sync:.2f} секунд")

    start_async = time.time()

    asyncio.run(main_async())

    end_async = time.time()

    print(f"Асинхронное выполнение: {end_async - start_async:.2f} секунд")