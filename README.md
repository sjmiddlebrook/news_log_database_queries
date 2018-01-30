# Logs Analysis Project
Project using SQL database queries for reporting tool.
## Getting Started
Run `python3 news_log.py` to run the project report.
## Custom Views
This project creates 2 custom views:
1. The first view saves the total error requests by date

`create view errors_by_date as select date(time) as view_date, count(status) as error_total from log where status = '404 NOT FOUND' group by view_date;`
2. The second view saves the total requests by date

`create view requests_by_date as select date(time) as view_date, count(status) as total from log group by view_date;`