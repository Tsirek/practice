from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import Practice
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


@app.get("/")
def read_items(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/parse")
def parse(text: str = '',
          schedule: list[str] | str = Query(None),
          salary: str = '0',
          only_with_salary: bool = False,
          experience: list[str] | str = Query(None),
          education: list[str] | str = Query(None)):
    if salary:
        salary = int(salary)
    else:
        salary = 0
    Practice.parse_and_store_data(text=text, schedule=schedule, salary=salary, only_with_salary=only_with_salary,experience=experience, education=education)
    Practice.print_analytics()
    return RedirectResponse(url='vacancies')


@app.get("/vacancies")
def result():
    return Practice.get_list_of_vacancy()
