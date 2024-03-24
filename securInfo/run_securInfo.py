import requests
from fake_useragent import UserAgent
import os
import shutil
import json
from datetime import datetime

# Основные параметры скрипта, которые используются в функциях
# Имя таблицы SQL
TBL_NAME = 'securInfo'

# Дата на которую нужно получить данные
REQUIRED_DATE = '01.03.2024'

# Результат проверки данных на дату. Переопределяется в функции get_code_page()
global CHECK_DATA
CHECK_DATA = None

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
    api_link_method = f"https://www.nationalclearingcentre.ru/api/v1/rates/securInfo?lang=ru&pageNumber=1&settleDate={REQUIRED_DATE}&secur=&isin=&fullCovered=&fullCoveredLimit=&otcDeals=&collateral=&gfUse=&sfUse=&gcIRisks=&gcCollateral=&gcBonds=&gcShares=&gcExpanded=&gcOfz=&gcMetal=&maturityDate=&collateralAcceptLimit="

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
        #"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Создаём сессию
    s = requests.Session()

    # Отправляем запрос на сервер для получения количества страниц, которые может вернуть метод
    get_total_page_method = s.get(api_link_method, headers=p_headers)

    # Проверяем ответ от сервера
    if get_total_page_method.status_code == 200:

        # Читаем полученный json и получаем общее количество страниц
        read_json = json.loads(get_total_page_method.text)
        total_page_method = read_json["totalPages"]

        # Если данных на дату нет, то завершаем работу функции
        if total_page_method == 0:
            print(f'На дату {REQUIRED_DATE} отсутствуют!')
            global CHECK_DATA
            CHECK_DATA = 0
            return

        # В цикле проходимся по всем страницам метода и сохраняем их в отдельные файлы
        for page_method in range(1, total_page_method + 1):
        #for page_method in range(1, 6):

            # Отправляем запрос на сервер
            api_page_method = f"https://www.nationalclearingcentre.ru/api/v1/rates/securInfo?lang=ru&pageNumber={page_method}&settleDate={REQUIRED_DATE}"
            get_page_method = s.get(api_page_method, headers=p_headers)

            # Получаем данные и преобразуем в читаемый JSON
            get_page_data = get_page_method.text
            read_data_json = json.loads(get_page_data)
            data_json = json.dumps(read_data_json, ensure_ascii=False, indent=4)

            # Сохраняем данные JSON в отдельный файл
            with open(f"json_file/page_{page_method}.json", mode="w", encoding="utf-8") as file:
                # file.write(get_page_method.text)
                file.write(data_json)

            print(f"Обработано страниц: {page_method}/{total_page_method}")

        else:
            print(f"Все страницы метода получены.")

    else:
        print(f'Что-то пошло не так! Получен следующий код HTTP: {get_total_page_method.status_code}')

# Функция для создания DDL-файла таблицы SQL
def create_tbl_sql():

    # Если данных на дату нет, то завершаем работу функции
    if CHECK_DATA == 0:
        return

    # Создаём DDL файл таблицы
    with open(f"result_file/create_tbl_{TBL_NAME}.ddl", mode="a+", encoding="utf-8") as create_tbl:

        # Удаление таблицы
        create_tbl.write(f'-- Удаление таблицы')
        create_tbl.write(f'\nDROP TABLE IF EXISTS {TBL_NAME};')

        # Создание таблицы
        create_tbl.write(f'\n-- Создание таблицы')
        create_tbl.write(f'\nCREATE TABLE {TBL_NAME} (')
        create_tbl.write(f'\n\tsettleDate date,')
        create_tbl.write(f'\n\ttradeDate date,')
        create_tbl.write(f'\n\tsecId text,')
        create_tbl.write(f'\n\tisin text,')
        create_tbl.write(f'\n\tshortName text,')
        create_tbl.write(f'\n\tfullCovered boolean default false,')
        create_tbl.write(f'\n\totcDeals boolean default false,')
        create_tbl.write(f'\n\tcollateral boolean default false,')
        create_tbl.write(f'\n\tcollateralAcceptLimit int4,')
        create_tbl.write(f'\n\tgfUse boolean default false,')
        create_tbl.write(f'\n\tsfUse boolean default false,')
        create_tbl.write(f'\n\tgciRisks boolean default false,')
        create_tbl.write(f'\n\tgcCollateral boolean default false,')
        create_tbl.write(f'\n\tgcBonds boolean default false,')
        create_tbl.write(f'\n\tgcShares boolean default false,')
        create_tbl.write(f'\n\tgcExpanded boolean default false,')
        create_tbl.write(f'\n\tgcOfz boolean default false,')
        create_tbl.write(f'\n\tgcMetal boolean default false,')
        create_tbl.write(f'\n\tmaturityDate date')
        create_tbl.write(f'\n);')

        # Добавление комментария к таблице
        create_tbl.write(f'\n-- Добавление комментария к таблице')
        create_tbl.write(f'\nCOMMENT ON TABLE {TBL_NAME} IS \'Параметры ценных бумаг. Метод securInfo (НКЦ).\';')

        # Добавление комментариев к столбцам таблицы
        create_tbl.write(f'\n-- Добавление комментариев к столбцам таблицы')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.settleDate is \'Дата запроса\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.tradeDate is \'Дата сделки\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.secId is \'Торговый код ЦБ\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.isin is \'ISIN ЦБ\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.shortName is \'Краткое наим. ЦБ\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.fullCovered is \'Запрет коротких продаж\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.otcDeals is \'Допущена к внебирж. сделкам с ЦК\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.collateral is \'Принимается в обеспепечение\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.collateralAcceptLimit is \'Принимается в обесп. Лимит приема в обеспечение, %\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gfUse is \'Принимается в ГФ\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.sfUse is \'Принимается в обеспеч. под стресс\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gciRisks is \'Принимается в обесп. GC IRisks\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gcCollateral is \'Принимается в обесп. GC Sovereign\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gcBonds is \'Принимается в обесп. GC Bonds\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gcShares is \'Принимается в обесп. GC Shares\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gcExpanded is \'Принимается в обесп. GC Expanded\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gcOfz is \'Принимается в обесп. OFZ\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.gcMetal is \'Принимается в обесп. GC METAL\';')
        create_tbl.write(f'\nCOMMENT ON COLUMN {TBL_NAME}.maturityDate is \'Дата погашения\';')

    print(f'DDL-файл для таблицы {TBL_NAME} готов.')

# Функция для чтения фалой JSON и создания файла с SQl-запросом
def read_code_page():

    # Если данных на дату нет, то завершаем работу функции
    if CHECK_DATA == 0:
        return

    # Открываем файл, в который будет записываться финальный sql-запрос
    with open(f"result_file/insert_{TBL_NAME}.sql", mode="a+", encoding="utf-8") as create_insert_sql:

        # Добавляем начальную строку оператора INSERT INTO
        create_insert_sql.write(f'INSERT INTO {TBL_NAME} (settleDate, tradeDate, secId, isin, shortName, fullCovered, otcDeals, collateral, collateralAcceptLimit, gfUse, sfUse, gciRisks, gcCollateral, gcBonds, gcShares, gcExpanded, gcOfz, gcMetal, maturityDate) VALUES ')

        # Выполняем сканирование директории "json_file". Затем открываем каждый файл и читаем его с последующей записью в финальный файл с sql-запросом
        for f in os.listdir("json_file"):

            # Открываем json-файл для чтения
            with open(f"json_file/{f}", mode="r", encoding="utf-8") as file_json:

                data = json.load(file_json)

                for i in range(0, len(data["rows"])):

                    #settleDate = json.dumps(data['rows'][i]['settleDate'], ensure_ascii=False)
                    get_settleDate = data['rows'][i]['settleDate'] # Преобразуем строку в формат datetime
                    get_settleDate_datetime_obj = datetime.strptime(get_settleDate, '%Y-%m-%dT%H:%M:%S') # Преобразуем формат datetime в требуемый шаблон
                    settleDate = "'" + get_settleDate_datetime_obj.strftime("%d.%m.%Y") + "'" # Преобразуем формат datetime в требуемый шаблон

                    # tradeDate = json.dumps(data['rows'][i]['tradeDate'], ensure_ascii=False)
                    get_tradeDate = data['rows'][i]['tradeDate']
                    get_tradeDate_datetime_obj = datetime.strptime(get_tradeDate, '%Y-%m-%dT%H:%M:%S')
                    tradeDate = "'" + get_tradeDate_datetime_obj.strftime("%d.%m.%Y") + "'"

                    secId = "'" + data['rows'][i]['secId'] + "'"
                    isin = "'" + data['rows'][i]['isin'] + "'"

                    # Заменяем ' на '', чтобы можно было добавлять значения с ' ковычкой в поле таблицы.
                    # Здесь будет ошибка: insert into таблица (поле) values ('BJ'sWholes');
                    # А здесь ошибки не будет: insert into таблица (поле) values ('BJ''sWholes');
                    shortName = "'" + data['rows'][i]['shortName'].replace("'", "''") + "'"

                    fullCovered = json.dumps(data['rows'][i]['fullCovered'], ensure_ascii=False)
                    otcDeals = json.dumps(data['rows'][i]['otcDeals'], ensure_ascii=False)
                    collateral = json.dumps(data['rows'][i]['collateral'], ensure_ascii=False)
                    collateralAcceptLimit = json.dumps(data['rows'][i]['collateralAcceptLimit'], ensure_ascii=False)
                    gfUse = json.dumps(data['rows'][i]['gfUse'], ensure_ascii=False)
                    sfUse = json.dumps(data['rows'][i]['sfUse'], ensure_ascii=False)
                    gciRisks = json.dumps(data['rows'][i]['gciRisks'], ensure_ascii=False)
                    gcCollateral = json.dumps(data['rows'][i]['gcCollateral'], ensure_ascii=False)
                    gcBonds = json.dumps(data['rows'][i]['gcBonds'], ensure_ascii=False)
                    gcShares = json.dumps(data['rows'][i]['gcShares'], ensure_ascii=False)
                    gcExpanded = json.dumps(data['rows'][i]['gcExpanded'], ensure_ascii=False)
                    gcOfz = json.dumps(data['rows'][i]['gcOfz'], ensure_ascii=False)
                    gcMetal = json.dumps(data['rows'][i]['gcMetal'], ensure_ascii=False)

                    # Поле maturityDate может быть null, а может содержать дату. Поэтому обрабатываем эти варианты.
                    get_maturityDate = json.dumps(data['rows'][i]['maturityDate'], ensure_ascii=False)

                    if get_maturityDate == 'null':
                        maturityDate = 'null'
                    else:
                        get_maturityDate_datetime_obj = datetime.strptime(get_maturityDate, '%Y-%m-%dT%H:%M:%S')
                        maturityDate = "'" + get_maturityDate_datetime_obj.strftime("%d.%m.%Y") + "'"

                    # Добавляем строку в оператор INSERT INTO
                    create_insert_sql.write(f"\n({settleDate}, {tradeDate}, {secId}, {isin}, {shortName}, {fullCovered}, {otcDeals}, {collateral}, {collateralAcceptLimit}, {gfUse}, {sfUse}, {gciRisks}, {gcCollateral}, {gcBonds}, {gcShares}, {gcExpanded}, {gcOfz}, {gcMetal}, {maturityDate}),")

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