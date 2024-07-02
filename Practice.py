 # библиотека

import requests
import pandas as pd
from sqlalchemy import create_engine


# БД
DATABASE_URI = 'sqlite:///jobs.db'
engine = create_engine(DATABASE_URI)

# Получение данных с хх
def fetch_hh_data(keyword, area=1, page=0):
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": keyword,
        "area": area,
        "page": page
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Проверка на ошибки
    return response.json()

# Парсинг и загрузка в Бд
def parse_and_store_data(keyword):
    data = fetch_hh_data(keyword)
    vacancies = data.get("items", [])

    # Загрузка в фрейм
    parsed_data = []
    for vacancy in vacancies:
        parsed_data.append({
            "id": vacancy["id"],
            "name": vacancy["name"],
            "employer": vacancy["employer"]["name"] if vacancy.get("employer") else None,
            "salary_from": vacancy["salary"]["from"] if vacancy.get("salary") else None,
            "salary_to": vacancy["salary"]["to"] if vacancy.get("salary") else None,
            "area": vacancy["area"]["name"] if vacancy.get("area") else None,
            "published_at": vacancy["published_at"]
        })
    
    df = pd.DataFrame(parsed_data)

    # Загрузка в бд
    df.to_sql("vacancies", con=engine, if_exists="append", index=False)

# Аналитика
def print_analytics():
    query = """
    SELECT
        COUNT(*) AS total_vacancies,
        COUNT(DISTINCT employer) AS unique_employers
    FROM vacancies
    """
    df = pd.read_sql(query, con=engine)
    print(df)

# Основной код
if __name__ == "__main__":
    # Пример использования
    keyword = "Python developer"
    parse_and_store_data(keyword)
    print_analytics()
