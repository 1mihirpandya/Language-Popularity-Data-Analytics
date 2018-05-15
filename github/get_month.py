#df = spark.read.format("csv").option("header", "true").load("csvfile.csv")
from pyparsing import col
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pprint import pprint



import datetime



# import pyspark.sql.functions.desc
import argparse
import re
WORD = re.compile(r"\d{4}-\d{2}-\d{2}")
global rdd

def setup_table(sc, sqlContext, commits_filename, repos_filename):
    df = sqlContext.read.csv(commits_filename, header=True, inferSchema=True)
    sqlContext.registerDataFrameAsTable(df, "commits")
    df2 = sqlContext.read.csv(repos_filename, header=True, inferSchema=True)
    sqlContext.registerDataFrameAsTable(df2, "repos")



def get_month(row):
    row[0] = row[0][5:7]
    return row

def lower_list(list):
    m = []
    for word in list:
        m.append(word.lower())
    return m


def check_lang(x, langs):
    retlangs = []
    check = 0
    given_lang_list = x[1].split(",")

    for lang in langs:
        check_w = lang.lower()
        if check_w in lower_list(given_lang_list):
            check = 1
            retlangs.append(lang)

    if check == 1:
        return x[0], retlangs
    return False



def month(sc, sqlContext, langs):
    '''
        for each month (in string format)
    '''
    commits = sqlContext.table("commits")
    repos = sqlContext.table("repos")
    commits = commits.withColumnRenamed("repo_name_group", "repo")
    joined_df = commits.join(repos, 'repo', 'inner')
    no_null_langs = joined_df.filter(joined_df.languages != "null")
    no_null_langs = no_null_langs.filter((joined_df.author_tz_offset == -480 )|
                                     (joined_df.author_tz_offset == -420) |
                                     (joined_df.author_tz_offset == -360 )|
                                     (joined_df.author_tz_offset == -300))
    #no_null_langs = no_null_langs.drop('repo').drop('author_tz_offset')

    #no_null_langs = no_null_langs.drop('repo')
    no_null_langs = no_null_langs.drop('repo').drop('author_tz_offset')

    to_rdd = no_null_langs.rdd
    rdd = to_rdd.map(list)
    fix_date = rdd.map(lambda x: get_month(x))

    fix_date = fix_date.map(lambda x: check_lang(x, langs))

    fix_date = fix_date.filter(lambda x: x)

    fix_date = fix_date.flatMapValues(lambda x: x)

    map_count = fix_date.map(lambda x: ((x[0], x[1]), 1))
    map_count = map_count.reduceByKey(lambda x, y: x + y)

    return map_count




if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('commits', help='File to load Amazon review data from')
    parser.add_argument('repos', help='File to load Yelp business data from')
    #parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()


    # Setup Spark
    conf = SparkConf().setAppName("gh_data_month")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)


    setup_table(sc, sqlContext, args.commits, args.repos)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    langs = {"Python", "Java", "JavaScript","Ruby", "SQL", "C#", "C++", "nodejs", "PHP", "C", "objective-c"}

    out = month(sc, sqlContext, langs)
    out.saveAsTextFile("/user/renukan2/month_github")
       print("-" * 30)
