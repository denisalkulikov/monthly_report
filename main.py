from nicegui import ui
import psycopg2
from dotenv import load_dotenv
import os
import warnings
import openpyxl
from openpyxl import load_workbook
import asyncio
import tempfile

load_dotenv()

warnings.filterwarnings('ignore')

# Конфигурация БД из переменных окружения
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "port": os.getenv("DB_PORT", "5432")
}

year = [
    2020, 2021, 2022, 2023, 2024, 2025, 2026
]

month = [
    'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
]

# Словарь для преобразования названия месяца в номер
month_to_number = {
    'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4,
    'Май': 5, 'Июнь': 6, 'Июль': 7, 'Август': 8,
    'Сентябрь': 9, 'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
}

# Словарь для преобразования названия месяца в именительный падеж с маленькой буквы
month_to_nominative_lower = {
    'Январь': 'январь',
    'Февраль': 'февраль',
    'Март': 'март',
    'Апрель': 'апрель',
    'Май': 'май',
    'Июнь': 'июнь',
    'Июль': 'июль',
    'Август': 'август',
    'Сентябрь': 'сентябрь',
    'Октябрь': 'октябрь',
    'Ноябрь': 'ноябрь',
    'Декабрь': 'декабрь'
}

# Словарь для преобразования названия месяца в предложный падеж
month_to_prepositional = {
    'Январь': 'январе',
    'Февраль': 'феврале',
    'Март': 'марте',
    'Апрель': 'апреле',
    'Май': 'мае',
    'Июнь': 'июне',
    'Июль': 'июле',
    'Август': 'августе',
    'Сентябрь': 'сентябре',
    'Октябрь': 'октябре',
    'Ноябрь': 'ноябре',
    'Декабрь': 'декабре'
}

# Словарь для преобразования названия месяца в букву колонки (A=1, B=2, etc.)
month_to_column = {
    1: 'B',   # Январь
    2: 'C',   # Февраль
    3: 'D',   # Март
    4: 'E',   # Апрель
    5: 'F',   # Май
    6: 'G',   # Июнь
    7: 'H',   # Июль
    8: 'I',   # Август
    9: 'J',   # Сентябрь
    10: 'K',  # Октябрь
    11: 'L',  # Ноябрь
    12: 'M'   # Декабрь
}

# Глобальные переменные для хранения данных и состояния UI
cached_data = None
cached_sales_data = None
cached_monthly_group_data = None
cached_sales_responsibility_data = None
current_results_container = None

def get_db_connection():
    """Создание подключения к БД"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        ui.notify(f'Ошибка подключения к БД: {e}', type='negative')
        return None

def fetch_data_by_direction(year_value, month_number):
    """Запрос для получения сумм по направлениям"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        
        query = """
        SELECT direction, SUM(pay_summ) as total_summ
        FROM kamtent.monthly_group_product
        WHERE year = %s AND month = %s
        GROUP BY direction
        ORDER BY direction
        """
        
        cur.execute(query, (year_value, month_number))
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return results
        
    except Exception as e:
        ui.notify(f'Ошибка при выполнении запроса: {e}', type='negative')
        return None

def fetch_data_for_directions(year_value, month_number):
    """Запрос для получения сумм по конкретным направлениям (с учетом регистра)"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        
        # Используем UPPER для поиска без учета регистра
        query = """
        SELECT direction, SUM(pay_summ) as total_summ
        FROM kamtent.monthly_group_product
        WHERE year = %s AND month = %s 
        AND UPPER(direction) IN ('ОАИ', 'ТК', 'АНГАРЫ', 'РЕКЛАМА', 'КН')
        GROUP BY direction
        """
        
        cur.execute(query, (year_value, month_number))
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Преобразуем в словарь для удобства
        result_dict = {row[0]: row[1] for row in results}
        
        # Добавим отладочный вывод
        print(f"Данные из БД для направлений: {result_dict}")
        
        return result_dict
        
    except Exception as e:
        ui.notify(f'Ошибка при выполнении запроса: {e}', type='negative')
        return None

def fetch_sales_data(year_value, month_number):
    """Запрос для получения данных из таблицы sales для листа Авто (РОЗНИЦА и ПОТРЕБИТЕЛИ)"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        
        # Запрос для получения суммы по РОЗНИЦА и ПОТРЕБИТЕЛИ (используем pay_date)
        query = """
        SELECT segment, SUM(pay_summ) as total_summ
        FROM kamtent.sales
        WHERE EXTRACT(YEAR FROM pay_date) = %s 
        AND EXTRACT(MONTH FROM pay_date) = %s
        AND direction = 'ОАИ'
        AND segment IN ('РОЗНИЦА', 'ПОТРЕБИТЕЛИ')
        GROUP BY segment
        """
        
        cur.execute(query, (year_value, month_number))
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Преобразуем в словарь для удобства
        result_dict = {row[0]: row[1] for row in results}
        
        # Добавим отладочный вывод
        print(f"Данные из sales для segment: {result_dict}")
        
        return result_dict
        
    except Exception as e:
        ui.notify(f'Ошибка при выполнении запроса к sales: {e}', type='negative')
        return None

def fetch_sales_responsibility_data(year_value, month_number):
    """Запрос для получения данных из таблицы sales по responsibility"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        
        # Запрос для получения суммы по responsibility (своя и чужая)
        query = """
        SELECT responsibility, SUM(pay_summ) as total_summ
        FROM kamtent.sales
        WHERE EXTRACT(YEAR FROM pay_date) = %s 
        AND EXTRACT(MONTH FROM pay_date) = %s
        AND direction = 'ОАИ'
        AND LOWER(responsibility) IN ('своя', 'чужая')
        GROUP BY responsibility
        """
        
        cur.execute(query, (year_value, month_number))
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Преобразуем в словарь для удобства
        result_dict = {row[0].upper(): row[1] for row in results}
        
        # Добавим отладочный вывод
        print(f"Данные из sales для responsibility: {result_dict}")
        
        return result_dict
        
    except Exception as e:
        ui.notify(f'Ошибка при выполнении запроса к sales (responsibility): {e}', type='negative')
        return None

def fetch_monthly_group_products(year_value, month_number):
    """Запрос для получения данных из monthly_group_product для листа Авто"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        
        # Запрос для получения сумм по конкретным group_product
        query = """
        SELECT group_product, SUM(pay_summ) as total_summ
        FROM kamtent.monthly_group_product
        WHERE year = %s AND month = %s 
        AND direction = 'ОАИ'
        AND group_product IN ('МСК', 'АВТОТЕНТЫ', 'АВТОУСЛУГИ', 'РЕМОНТ', 'ПРОЧЕЕ', 'АВТОКАРКАСЫ', 'ВОРОТА', 'АВТОПОЛОГИ')
        GROUP BY group_product
        """
        
        cur.execute(query, (year_value, month_number))
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Преобразуем в словарь для удобства
        result_dict = {row[0]: row[1] for row in results}
        
        # Добавим отладочный вывод
        print(f"Данные из monthly_group_product для group_product: {result_dict}")
        
        return result_dict
        
    except Exception as e:
        ui.notify(f'Ошибка при выполнении запроса к monthly_group_product: {e}', type='negative')
        return None

def fetch_group_products(year_value, month_number, direction):
    """Запрос для получения group_product по конкретному направлению"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        
        query = """
        SELECT group_product, pay_summ
        FROM kamtent.monthly_group_product
        WHERE year = %s AND month = %s AND direction = %s
        ORDER BY group_product
        """
        
        cur.execute(query, (year_value, month_number, direction))
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return results
        
    except Exception as e:
        ui.notify(f'Ошибка при выполнении запроса: {e}', type='negative')
        return None

def create_expandable_row(direction, direction_total, year_value, month_number):
    """Создает раскрывающуюся строку для направления"""
    with ui.expansion(f'{direction} — {direction_total:,.2f} руб.', icon='folder').classes('w-full mb-2'):
        content_container = ui.column()
        
        with content_container:
            with ui.row().classes('items-center gap-2'):
                ui.spinner(size='sm')
                ui.label('Загрузка данных...').classes('text-grey-6')
        
        group_products = fetch_group_products(year_value, month_number, direction)
        
        content_container.clear()
        
        with content_container:
            if group_products and len(group_products) > 0:
                with ui.row().classes('justify-between items-center mb-2 p-2 bg-grey-1 rounded'):
                    ui.label('Общая сумма направления:').classes('text-subtitle1 font-bold')
                    ui.label(f'{direction_total:,.2f} руб.').classes('text-subtitle1 font-bold text-primary')
                
                columns = [
                    {'name': 'group_product', 'label': 'Группа продуктов', 'field': 'group_product', 'align': 'left'},
                    {'name': 'pay_summ', 'label': 'Сумма, руб.', 'field': 'pay_summ', 'align': 'right'}
                ]
                
                rows = []
                subtotal = 0
                
                for product, summ in group_products:
                    if summ:
                        rows.append({'group_product': product, 'pay_summ': f'{float(summ):,.2f}'})
                        subtotal += float(summ)
                    else:
                        rows.append({'group_product': product, 'pay_summ': '0.00'})
                
                ui.table(columns=columns, rows=rows, row_key='group_product').classes('w-full')
                
                with ui.row().classes('mt-2 justify-end w-full'):
                    ui.label(f'Сумма по группам:').classes('text-subtitle2')
                    ui.label(f'{subtotal:,.2f} руб.').classes('text-subtitle2 text-primary')
                    
                if abs(subtotal - direction_total) > 0.01:
                    with ui.row().classes('mt-1 justify-end w-full'):
                        ui.label(f'(Расхождение: {direction_total - subtotal:,.2f} руб.)').classes('text-caption text-grey-6')
            else:
                ui.label('Нет данных по продуктам в этом направлении').classes('text-grey-6')

def process_excel_file(temp_file_path, selected_month_name, selected_year, directions_data, sales_data, monthly_group_data, sales_responsibility_data, client):
    """Обработка Excel файла"""
    output_path = None
    try:
        # Загружаем workbook из временного файла
        wb = load_workbook(temp_file_path, keep_vba=True)
        
        # 1. Обработка листа "служ"
        if "служ" in wb.sheetnames:
            sheet_serv = wb["служ"]
            
            # Получаем данные для заполнения
            month_number = month_to_number[selected_month_name]
            month_nominative = month_to_nominative_lower[selected_month_name]
            month_prepositional = month_to_prepositional[selected_month_name]
            
            # Заполняем ячейки
            sheet_serv['B1'] = f"{month_nominative} "
            sheet_serv['B2'] = f"{month_prepositional} "
            sheet_serv['B3'] = str(month_number)
            sheet_serv['B4'] = str(selected_year)
        else:
            with client:
                ui.notify('Лист "служ" не найден в файле', type='warning')
        
        # 2. Обработка листа "тит"
        if "тит" in wb.sheetnames:
            sheet_tit = wb["тит"]
            sheet_tit['F44'] = str(selected_year)
        else:
            with client:
                ui.notify('Лист "тит" не найден в файле', type='warning')
        
        # 3. Обработка листа "общ"
        if "общ" in wb.sheetnames:
            sheet_obsh = wb["общ"]
            month_number = month_to_number[selected_month_name]
            column_letter = month_to_column[month_number]
            
            # Сопоставление направлений (учитываем возможный регистр в БД)
            direction_cells = {
                'ОАИ': '40',
                'ТК': '41',
                'АНГАРЫ': '43',
                'РЕКЛАМА': '47',
                'КН': '49'
            }
            
            for direction, row in direction_cells.items():
                cell = f"{column_letter}{row}"
                # Ищем направление в данных (с учетом регистра)
                amount = 0
                for db_direction, db_amount in directions_data.items():
                    if db_direction.upper() == direction:
                        amount = float(db_amount) if db_amount else 0
                        break
                
                # Добавим отладочный вывод
                print(f"Записываю в ячейку {cell} для направления '{direction}' сумму: {amount}")
                
                # Записываем число
                sheet_obsh[cell] = amount
        else:
            with client:
                ui.notify('Лист "общ" не найден в файле', type='warning')
        
        # 4. Обработка листа "Авто"
        if "Авто" in wb.sheetnames:
            sheet_auto = wb["Авто"]
            
            # Получаем суммы для РОЗНИЦА и ПОТРЕБИТЕЛИ из sales
            roznica_amount = sales_data.get('РОЗНИЦА', 0)
            potrebiteli_amount = sales_data.get('ПОТРЕБИТЕЛИ', 0)
            
            # Записываем в ячейки
            sheet_auto['R5'] = float(roznica_amount) if roznica_amount else 0
            sheet_auto['R7'] = float(potrebiteli_amount) if potrebiteli_amount else 0
            
            print(f"Записываю в ячейку R5 (РОЗНИЦА) сумму: {roznica_amount}")
            print(f"Записываю в ячейку R7 (ПОТРЕБИТЕЛИ) сумму: {potrebiteli_amount}")
            
            # Получаем данные из monthly_group_product
            group_product_cells = {
                'АВТОТЕНТЫ': 'R36',
                'АВТОПОЛОГИ': 'R37',
                'АВТОКАРКАСЫ': 'R38',
                'ВОРОТА': 'R39',
                'АВТОУСЛУГИ': 'R42',
                'МСК': 'R43',
                'РЕМОНТ': 'R44',
                'ПРОЧЕЕ': 'R45'
            }
            
            for group_product, cell in group_product_cells.items():
                amount = monthly_group_data.get(group_product, 0)
                sheet_auto[cell] = float(amount) if amount else 0
                print(f"Записываю в ячейку {cell} для group_product '{group_product}' сумму: {amount}")
            
            # Получаем данные из sales по responsibility
            svoya_amount = sales_responsibility_data.get('СВОЯ', 0)
            chuzhaya_amount = sales_responsibility_data.get('ЧУЖАЯ', 0)
            
            # Записываем в ячейки R74 и R75
            sheet_auto['R74'] = float(svoya_amount) if svoya_amount else 0
            sheet_auto['R75'] = float(chuzhaya_amount) if chuzhaya_amount else 0
            
            print(f"Записываю в ячейку R74 (СВОЯ) сумму: {svoya_amount}")
            print(f"Записываю в ячейку R75 (ЧУЖАЯ) сумму: {chuzhaya_amount}")
            
        else:
            with client:
                ui.notify('Лист "Авто" не найден в файле', type='warning')
        
        # Формируем имя для сохранения
        output_filename = f"Самара {month_number} Отчет {selected_year} {selected_month_name}.xlsm"
        
        # Сохраняем во временную папку
        temp_dir = tempfile.gettempdir()
        output_path = os.path.join(temp_dir, output_filename)
        wb.save(output_path)
        wb.close()
        
        with client:
            ui.notify(f'Файл успешно создан: {output_filename}', type='positive')
            # Предлагаем скачать файл
            ui.download(output_path)
        
    except Exception as e:
        with client:
            ui.notify(f'Ошибка при обработке файла: {e}', type='negative')
        import traceback
        traceback.print_exc()
    finally:
        # Удаляем временный файл (исходный загруженный файл)
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass

async def handle_file_upload(e, selected_month_name, selected_year, directions_data, sales_data, monthly_group_data, sales_responsibility_data):
    """Асинхронная обработка загрузки файла"""
    if not selected_month_name or not selected_year:
        with ui.context.client:
            ui.notify('Сначала выберите год и месяц', type='warning')
        return
    
    # Получаем клиент из контекста события
    client = e.client
    
    # Создаем временный файл с правильным расширением
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsm') as tmp_file:
        # Асинхронно читаем файл
        file_content = await e.file.read()
        tmp_file.write(file_content)
        temp_file_path = tmp_file.name
    
    # Обрабатываем файл
    process_excel_file(temp_file_path, selected_month_name, selected_year, directions_data, sales_data, monthly_group_data, sales_responsibility_data, client)

def on_button_click():
    """Обработчик нажатия кнопки получения данных"""
    global cached_data, cached_sales_data, cached_monthly_group_data, cached_sales_responsibility_data, current_results_container
    
    selected_year = select_year.value
    selected_month_name = select_month.value
    selected_month_number = month_to_number[selected_month_name]
    
    ui.notify(f'Загружаю данные за {selected_month_name} {selected_year}...', type='info')
    
    data = fetch_data_by_direction(selected_year, selected_month_number)
    
    # Сохраняем данные для использования при загрузке файла
    directions_data = fetch_data_for_directions(selected_year, selected_month_number)
    sales_data = fetch_sales_data(selected_year, selected_month_number)
    monthly_group_data = fetch_monthly_group_products(selected_year, selected_month_number)
    sales_responsibility_data = fetch_sales_responsibility_data(selected_year, selected_month_number)
    cached_data = directions_data
    cached_sales_data = sales_data
    cached_monthly_group_data = monthly_group_data
    cached_sales_responsibility_data = sales_responsibility_data
    
    # Отладочный вывод
    print(f"Сохраненные данные для направлений: {cached_data}")
    print(f"Сохраненные данные из sales: {cached_sales_data}")
    print(f"Сохраненные данные из monthly_group_product: {cached_monthly_group_data}")
    print(f"Сохраненные данные из sales (responsibility): {cached_sales_responsibility_data}")
    
    # Очищаем и пересоздаем контейнер результатов
    if current_results_container:
        current_results_container.clear()
    
    with result_container:
        if data and len(data) > 0:
            ui.label(f'Результаты за {selected_month_name} {selected_year}:').classes('text-h6 mb-4')
            
            total_all = 0
            
            for direction, total_summ in data:
                if total_summ:
                    direction_total = float(total_summ)
                    total_all += direction_total
                    create_expandable_row(direction, direction_total, selected_year, selected_month_number)
                else:
                    create_expandable_row(direction, 0.0, selected_year, selected_month_number)
            
            ui.separator()
            with ui.row().classes('mt-4 justify-end w-full'):
                ui.label('Общая сумма по всем направлениям:').classes('text-h6 font-bold')
                ui.label(f'{total_all:,.2f} руб.').classes('text-h6 font-bold text-primary')
        else:
            ui.label(f'Нет данных за {selected_month_name} {selected_year}').classes('text-body1 text-grey-8')
    
    current_results_container = result_container

# Создаем интерфейс
with ui.card().classes('w-full p-4 mb-4'):
    with ui.row().classes('items-end gap-4'):
        select_year = ui.select(options=year, value=2026, label='Выберите год').classes('w-40')
        select_month = ui.select(options=month, value='Январь', label='Выберите месяц').classes('w-40')
        ui.button('Получить данные', on_click=on_button_click, icon='search').classes('bg-primary text-white')
        
        # Кнопка для загрузки файла
        upload_btn = ui.upload(
            label='Выбрать файл XLSM',
            on_upload=lambda e: asyncio.create_task(handle_file_upload(
                e, 
                select_month.value, 
                select_year.value,
                cached_data,
                cached_sales_data,
                cached_monthly_group_data,
                cached_sales_responsibility_data
            )),
            auto_upload=True
        ).classes('w-auto')
        upload_btn.props('accept=".xlsm"')

# Создаем пустой контейнер для результатов
result_container = ui.column().classes('w-full')

ui.run(title='Составление ежемесячного отчёта', reload=True, port=8080)