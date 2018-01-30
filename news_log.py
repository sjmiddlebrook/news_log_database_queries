#!/usr/bin/python3
import psycopg2

DBNAME = "news"


def create_errors_view():
    """Create view for error report that totals the errors by date"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "create view errors_by_date as select date(time) as view_date, "
        "count(status) as error_total from log where status = '404 NOT FOUND' "
        "group by view_date")
    db.commit()
    db.close()


def create_requests_view():
    """Create view for error report that totals the requests by date"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "create view requests_by_date as select date(time) as view_date, "
        "count(status) as total from log group by view_date")
    db.commit()
    db.close()


def get_top_three_articles():
    """Returns the 3 most popular articles of all time"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "select articles.title, count(log.path) as total_views "
        "from articles, log where articles.slug = "
        "substring(log.path, '[^\/]+$') group by articles.title "
        "order by total_views desc limit 3")
    top_articles = c.fetchall()
    db.close()
    return top_articles


def get_top_authors():
    """Returns the most popular authors of all time"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "select authors.name, count(log.path) as total_pageviews "
        "from authors, articles, log where authors.id = articles.author "
        "and articles.slug = substring(log.path, '[^\/]+$') "
        "group by authors.name order by total_pageviews desc")
    top_authors = c.fetchall()
    db.close()
    return top_authors


def get_top_error_days():
    """Returns the days where more than 1% of requests led to errors"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(
        "select requests_by_date.view_date, "
        "round(cast(errors_by_date.error_total as decimal) / "
        "requests_by_date.total * 100, 3) as percent_error "
        "from requests_by_date, errors_by_date "
        "where requests_by_date.view_date = errors_by_date.view_date "
        "and round(cast(errors_by_date.error_total as decimal) / "
        "requests_by_date.total * 100, 3) > 1")
    error_days = c.fetchall()
    db.close()
    return error_days


# create the two views needed for the queries
create_errors_view()
create_requests_view()

# print out the 3 most popular articles
print("Three most popular articles of all time:")
articles = get_top_three_articles()
for article in articles:
    print("    " + article[0] + " - " + str(article[1]) + " views")
print("")

# print out the authors with the most pageviews
print("Most popular authors by pageviews:")
authors = get_top_authors()
for author in authors:
    print("    " + author[0] + " - " + str(author[1]) + " views")
print("")

# print out the days where more than 1% of requests led to errors
print("Days where more than 1% of requests led to errors:")
days = get_top_error_days()
for day in days:
    print("    " + day[0].strftime("%B %d, %Y") + " - " +
          str(day[1]) + " errors")
print("")
