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


def get_date(row, langs):
    #Month/Day/Year
    row[0] = row[0][5:7] + "/" + row[0][8:10] + "/" + row[0][0:4]

    #row = check_lang(row, langs)
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


#    #joining the commits table with the repositories table on the repository name
#    joined_df = commits.join(repos, 'repo', 'inner')
#    ...
#    #pulling the languages we are tracking from the language tag
#    fix_date = fix_date.map(lambda x: check_lang(x, langs))
#    ...
#    #counting the key ((date, language), count)
#    map_count = map_count.reduceByKey(lambda x, y: x + y)







def year(sc, sqlContext, langs):
    '''
    gets the language count for every day in the year(in string format)
    '''
    commits = sqlContext.table("commits")
    repos = sqlContext.table("repos")
    commits = commits.withColumnRenamed("repo_name_group", "repo")
    
    #joining the commits table with the repositories table on the repository name
    joined_df = commits.join(repos, 'repo', 'inner')
    no_null_langs = joined_df.filter(joined_df.languages != "null")
    us_data = no_null_langs.filter((joined_df.author_tz_offset == -480) |
                                     (joined_df.author_tz_offset == -420) |
                                     (joined_df.author_tz_offset == -360) |
                                     (joined_df.author_tz_offset == -300))
    final_table = us_data.drop('repo').drop('author_tz_offset')

    to_rdd = final_table.rdd

    rdd = to_rdd.map(list)
    fix_date = rdd.map(lambda x: get_date(x, langs))
    get_langs = fix_date.map(lambda x: check_lang(x, langs))

    get_langs = get_langs.filter(lambda x: x)

    final_lang = get_langs.flatMapValues(lambda x: x)

    map_count = final_lang.map(lambda x: ((x[0], x[1]), 1))

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
    conf = SparkConf().setAppName("gh_data_year")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)


    setup_table(sc, sqlContext, args.commits, args.repos)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    langs = {"Python", "Java", "JavaScript","Ruby", "SQL", "C#", "C++", "nodejs", "PHP", "C", "objective-c"}

    out2 = year(sc, sqlContext, langs)
    out2.saveAsTextFile("/user/renukan2/year_github")

    print("-" * 30)
