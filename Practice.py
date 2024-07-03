 # библиотека

import requests
import pandas as pd
import pandas.io.sql as psql
from sqlalchemy import create_engine


# БД
DATABASE_URI = 'postgresql+psycopg2://postgres:admin@db/jobs'
engine = create_engine(DATABASE_URI)

# Получение данных с хх
def fetch_hh_data(text='', schedule='', salary=1, only_with_salary=False,experience='', education='', area=1, page=0):
    print(text, schedule, salary, only_with_salary, education)
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": text,
        'only_with_salary': only_with_salary,
        "area": area,
        "page": page
    }
    if schedule:
        params['schedule'] = schedule
    if salary > 0:
        params['salary'] = salary
    if experience:
        params['experience'] = experience
    if education:
        params['education'] = education
    response = requests.get(url, params=params)
    response.raise_for_status()  # Проверка на ошибки
    return response.json()

# Парсинг и загрузка в Бд
def parse_and_store_data(text='', schedule='', salary=1, only_with_salary=False,experience='', education='', area=1, page=0):
    try:
        c = engine.connect()
        query = """DELETE FROM vacancies;select * from vacancies limit 1;"""
        psql.read_sql_query(query, con=c)
        c.commit()
        c.close()
    except Exception as ex:
        print(ex)
    data = fetch_hh_data(text=text, schedule=schedule, salary=salary, only_with_salary=only_with_salary,experience=experience, education=education)
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

def get_list_of_vacancy():
    query = """
    SELECT * FROM vacancies
    """
    df = pd.read_sql(query, con=engine)
    list_of_vacancy = []
    for key in df:
        for i in range(len(df[key])):
            if len(list_of_vacancy) <= i:
                list_of_vacancy += [{}]
            if key == 'salary_to' or key == 'salary_from':
                if str(df[key][i]) == 'nan':
                    list_of_vacancy[i][key] = None
                else:
                    list_of_vacancy[i][key] = float(df[key][i])
            else:
                list_of_vacancy[i][key] = df[key][i]
    return list_of_vacancy

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
    keyword = ""
    parse_and_store_data(keyword)
    print_analytics()
