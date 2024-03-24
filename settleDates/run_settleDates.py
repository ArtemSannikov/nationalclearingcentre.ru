import requests
from fake_useragent import UserAgent
import os
import shutil
import json
from datetime import datetime

# Основные параметры скрипта, которые используются в функциях
# Имя таблицы SQL
TBL_NAME = 'settleDates'

# Год на который нужно получить данные
REQUIRED_YEAR = '2024'

# Проверяем наличие необходимых директорий, если они отсутствуют, то создаем.
# Если они уже существуют, то удаляем в них всё содержимое.
def check_dir():

    # Список директорий, который должен быть в корне метода securInfo
    list_dir = ["json_file", "result_file"]

    for name_dir in list_dir:

        # Проверяем, существует ли директория в корне
        if os.path.isdir(name_dir):

            # Выполняем сканирование каждой директории на наличие файлов и каталогов
            for filename in os.listdir(name_dir):

                # Получаем относительный путь до файла в директории
                current_file_path = os.path.join(name_dir, filename)

                # Выполняем удаление лишних объектов в зависимости от их типа (файл или директория)
                try:
                    if os.path.isfile(current_file_path) or os.path.islink(current_file_path):
                        os.unlink(current_file_path)
                    elif os.path.isdir(current_file_path):
                        shutil.rmtree(current_file_path)
                except Exception as e:
                    print('Ошибка при удалении %s. Причина: %s' % (current_file_path, e))
        else:
            # Если директория из списка list_dir отсутствует, создаем её
            os.mkdir(name_dir)
    else:
        print(f'Проверка директорий завершена.')

# Формируем запрос и отправляем его на сервер www.nationalclearingcentre.ru
# Полученный результат записываем в файл
def get_code_page():

    # Ссылка на API метода
    api_link_method = f"https://iss.moex.com/iss/rms/engines/currency/objects/settlementscalendar.json?lang=ru&year={REQUIRED_YEAR}"

    # Формируем фейковый user-agent для запросов. Доступные браузеры ["chrome", "edge", "firefox", "safari"]
    fake_array_browser = UserAgent(browsers=["chrome", "firefox", "safari"])
    fake_browser = fake_array_browser.random

    # Формируем заголовки для запроса
    p_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,la;q=0.6",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Encoding": "gzip",
        "Cache-Control": "no-cache",
        "User-Agent": fake_browser
    }

    # Создаём сессию
    s = requests.Session()

    # Отправляем запрос на сервер для получения количества страниц, которые может вернуть метод
    get_page_method = s.get(api_link_method, headers=p_headers)

    # Проверяем ответ от сервера
    if get_page_method.status_code == 200:

        # Получаем данные и преобразуем в читаемый JSON
        get_page_data = get_page_method.text
        read_data_json = json.loads(get_page_data)
        data_json = json.dumps(read_data_json, ensure_ascii=False, indent=4)

        # Сохраняем данные JSON в отдельный файл
        with open(f"json_file/page_1.json", mode="w", encoding="utf-8") as file:
            # file.write(get_page_method.text)
            file.write(data_json)
    else:
        print(f'Что-то пошло не так! Получен следующий код HTTP: {get_page_method.status_code}')

# Функция для создания DDL-файла таблицы SQL
def create_tbl_sql():

    # Создаём DDL файл таблицы
    with open(f"result_file/create_tbl_{TBL_NAME}.ddl", mode="a+", encoding="utf-8") as create_tbl:

        # Удаление таблицы
        create_tbl.write(f'-- Удаление таблицы')
        create_tbl.write(f'\nDROP TABLE IF EXISTS {TBL_NAME};')

        # Создание таблицы
        create_tbl.write(f'\n-- Создание таблицы')
        create_tbl.write(f'\nCREATE TABLE {TBL_NAME} (')
        create_tbl.write(f'\n\ttradedate date,')
        create_tbl.write(f'\n\tcurrencyid text,')
        create_tbl.write(f'\n\tcurrency_title text,')
        create_tbl.write(f'\n\tcurrency_market int2,')
        create_tbl.write(f'\n\tstock_market int2')
        create_tbl.write(f'\n);')

        # Добавление комментария к таблице
        create_tbl.write(f'\n-- Добавление комментария к таблице')
        create_tbl.write(f'\nCOMMENT ON TABLE {TBL_NAME} IS \'Календарь расчетных дней\';')

        # Добавление комментариев к столбцам таблицы
        create_tbl.write(f'\n-- Добавление комментариев к столбцам таблицы')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.tradedate is \'Дата в календаре\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.currencyid is \'Код валюты\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.currency_title is \'Краткое наименование\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.currency_market is \'Расчётный день по календарю ВР (валютного рынка)\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.stock_market is \'Расчётный день по календарю ФР (фондового рынка)\';')

    print(f'DDL-файл для таблицы {TBL_NAME} готов.')

# Функция для чтения фалой JSON и создания файла с SQl-запросом
def read_code_page():

    # Открываем файл, в который будет записываться финальный sql-запрос
    with open(f"result_file/insert_{TBL_NAME}.sql", mode="a+", encoding="utf-8") as create_insert_sql:

        # Добавляем начальную строку оператора INSERT INTO
        create_insert_sql.write(f'INSERT INTO {TBL_NAME} (tradedate, currencyid, currency_title, currency_market, stock_market) VALUES ')

        # Выполняем сканирование директории "json_file". Затем открываем каждый файл и читаем его с последующей записью в финальный файл с sql-запросом
        for f in os.listdir("json_file"):

            # Открываем json-файл для чтения
            with open(f"json_file/{f}", mode="r", encoding="utf-8") as file_json:

                data = json.load(file_json)

                for i in range(0, len(data["settlements_calendar"]["data"])):

                    # Заменяем ' на '', чтобы можно было добавлять значения с ' ковычкой в поле таблицы.
                    # Здесь будет ошибка: insert into таблица (поле) values ('BJ'sWholes');
                    # А здесь ошибки не будет: insert into таблица (поле) values ('BJ''sWholes');
                    tradeDate = "'" + data["settlements_calendar"]["data"][i][0].replace("\"", "") + "'"
                    currencyid = "'" + data["settlements_calendar"]["data"][i][1].replace("\"", "") + "'"

                    # Поле currency_title может быть null, а может содержать текст. Поэтому обрабатываем эти варианты.
                    get_currency_title = json.dumps(data["settlements_calendar"]["data"][i][2], ensure_ascii=False)
                    if get_currency_title == 'null':
                        currency_title = 'null'
                    else:
                        currency_title = "'" + get_currency_title.replace("\"", "") + "'"

                    currency_market = data["settlements_calendar"]["data"][i][3]
                    stock_market = data["settlements_calendar"]["data"][i][4]

                    # Добавляем строку в оператор INSERT INTO
                    create_insert_sql.write(f"\n({tradeDate}, {currencyid}, {currency_title}, {currency_market}, {stock_market}),")

        else:
            print(f"Исходные файлы JSON обработаны.")

        # Удаляем лишнюю "запятую" в конце запроса после обработки JSON файлов.
        create_insert_sql.read()
        create_insert_sql.seek(0, 2) # Перемещаем курсор в конец файла
        create_insert_sql.seek(create_insert_sql.tell() - 1) # Перемещаем курсор на последний символ
        create_insert_sql.truncate() # Удаляем последний символ

        # Добавляем символ ";" в конец запроса.
        create_insert_sql.write(f'\n;')
        print(f'Файл result_file/insert_{TBL_NAME}.sql сформирован.')

def main():
    check_dir()
    get_code_page()
    create_tbl_sql()
    read_code_page()

if __name__ == '__main__':
    main()