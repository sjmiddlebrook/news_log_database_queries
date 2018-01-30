import psycopg2

DBNAME = "news"


def create_errors_view():
    """Create views for error report"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "create view errors_by_date as select date(time) as view_date, count(status) as error_total from log where status = '404 NOT FOUND' group by view_date")
    db.commit()
    db.close()


def create_requests_view():
    """Create views for error report"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "create view requests_by_date as select date(time) as view_date, count(status) as total from log group by view_date")
    db.commit()
    db.close()


def get_top_three_articles():
    """Returns the 3 most popular articles of all time"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "select articles.title from articles, log where articles.slug = substring(log.path, '[^\/]+$') group by articles.title order by count(log.path) desc limit 3")
    top_articles = c.fetchall()
    db.close()
    return top_articles


def get_top_authors():
    """Returns the most popular authors of all time"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "select authors.name from authors, articles, log where authors.id = articles.author and articles.slug = substring(log.path, '[^\/]+$') group by authors.name order by count(log.path) desc")
    top_authors = c.fetchall()
    db.close()
    return top_authors


def get_top_error_days():
    """Returns the days where more than 1% of requests led to errors"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "select requests_by_date.view_date from requests_by_date, errors_by_date where requests_by_date.view_date = errors_by_date.view_date and round(cast(errors_by_date.error_total as decimal) / requests_by_date.total * 100, 3) > 1")
    error_days = c.fetchall()
    db.close()
    return error_days


create_errors_view()
create_requests_view()
articles = get_top_three_articles()
print(articles)
authors = get_top_authors()
print(authors)
days = get_top_error_days()
print(days)
