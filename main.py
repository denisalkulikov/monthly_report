from nicegui import ui
import psycopg2
from dotenv import load_dotenv
import os
import warnings
import openpyxl
from openpyxl import load_workbook
from datetime import datetime
import shutil
import tempfile
import asyncio

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

# Словарь для преобразования названия месяца в родительный падеж
month_to_genitive = {
    'Январь': 'января',
    'Февраль': 'февраля',
    'Март': 'марта',
    'Апрель': 'апреля',
    'Май': 'мая',
    'Июнь': 'июня',
    'Июль': 'июля',
    'Август': 'августа',
    'Сентябрь': 'сентября',
    'Октябрь': 'октября',
    'Ноябрь': 'ноября',
    'Декабрь': 'декабря'
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

def process_excel_file(file_content, file_name, selected_month_name, selected_year, client):
    """Обработка Excel файла"""
    temp_file_path = None
    try:
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsm') as tmp_file:
            tmp_file.write(file_content)
            temp_file_path = tmp_file.name
        
        # Загружаем workbook
        wb = load_workbook(temp_file_path, keep_vba=True)
        
        # Выбираем лист "служ"
        if "служ" in wb.sheetnames:
            sheet = wb["служ"]
        else:
            with client:
                ui.notify('Лист "служ" не найден в файле', type='negative')
            return
        
        # Получаем данные для заполнения
        month_number = month_to_number[selected_month_name]
        month_prepositional = month_to_prepositional[selected_month_name]
        
        # Заполняем ячейки
        sheet['B1'] = f"{selected_month_name} "  # с пробелом
        sheet['B2'] = f"{month_prepositional} "  # с пробелом
        sheet['B3'] = str(month_number)  # без пробела
        sheet['B4'] = str(selected_year)  # без пробела
        
        # Формируем имя для сохранения
        output_filename = f"Самара {month_number} Отчет {selected_year} {selected_month_name}.xlsm"
        
        # Сохраняем файл
        output_path = os.path.join(os.getcwd(), output_filename)
        wb.save(output_path)
        wb.close()
        
        with client:
            ui.notify(f'Файл успешно сохранен как: {output_filename}', type='positive')
            # Предлагаем скачать файл
            ui.download(output_path)
        
    except Exception as e:
        with client:
            ui.notify(f'Ошибка при обработке файла: {e}', type='negative')
        import traceback
        traceback.print_exc()
    finally:
        # Удаляем временный файл
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass

async def on_file_upload(e, selected_month_name, selected_year):
    """Обработчик загрузки файла (асинхронный)"""
    if not selected_month_name or not selected_year:
        ui.notify('Сначала выберите год и месяц', type='warning')
        return
    
    # Получаем текущего клиента
    client = ui.context.client
    
    # Получаем информацию о загруженном файле
    file_content = await e.file.read()
    file_name = e.file.name
    
    process_excel_file(file_content, file_name, selected_month_name, selected_year, client)

def on_button_click():
    """Обработчик нажатия кнопки получения данных"""
    selected_year = select_year.value
    selected_month_name = select_month.value
    selected_month_number = month_to_number[selected_month_name]
    
    ui.notify(f'Загружаю данные за {selected_month_name} {selected_year}...', type='info')
    
    data = fetch_data_by_direction(selected_year, selected_month_number)
    
    result_container.clear()
    
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

# Создаем интерфейс
with ui.card().classes('w-full p-4 mb-4'):
    with ui.row().classes('items-end gap-4'):
        select_year = ui.select(options=year, value=2026, label='Выберите год').classes('w-40')
        select_month = ui.select(options=month, value='Январь', label='Выберите месяц').classes('w-40')
        ui.button('Получить данные', on_click=on_button_click, icon='search').classes('bg-primary text-white')
        
        # Кнопка для загрузки файла
        upload_btn = ui.upload(
            label='Выбрать файл XLSM',
            on_upload=lambda e: asyncio.create_task(on_file_upload(
                e, 
                select_month.value, 
                select_year.value
            )),
            auto_upload=True
        ).classes('w-auto')
        upload_btn.props('accept=".xlsm"')

# Создаем пустой контейнер для результатов
result_container = ui.column().classes('w-full')

ui.run(title='Составление ежемесячного отчёта', reload=True, port=8080)