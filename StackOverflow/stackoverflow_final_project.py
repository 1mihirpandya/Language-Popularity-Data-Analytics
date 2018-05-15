from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import datetime
import csv


def find_engaging_reviews(sc, sqlContext, user_file, post_file):
    '''
    '''
    user_data = sqlContext.read.csv(user_file).rdd
    simplified_user_data = user_data.map(lambda x: get_user_pairs(x)).filter(lambda x: ("#" != x[0] and x[1] != None))
    # hdfs:///projects/group9/posts_questions.csv
    post_data = sqlContext.read.csv(post_file).rdd
    simplified_post_data = post_data.map(lambda x: get_post_pairs(x)).filter(lambda x: "#" != x[0])
    #structure of joined data is (uid, (location, (date, tags)))
    #                                                                                //location,   (date,        tags)
    joined_data = simplified_user_data.join(simplified_post_data).map(lambda x: (x[1][0], (x[1][1][0], x[1][1][1])))
    #both date and tags are an array
    #date is in the form [month, day, year]
    return joined_data

def query_5(sc, joined_data):
    data = joined_data.flatMap(lambda x: [((tag, x[0]), 1) for tag in x[1][1]])
    data = data.map(lambda x: query_5_restructure(x)).filter(lambda x: x[0] != '#')
    data = data.reduceByKey(lambda x, y: x + y)
    data = data.map(lambda x: (x[0][0], (x[0][1], x[1])))
    data = data.groupByKey().mapValues(list)
    return data

def query_5_restructure(entry):
    pst = ["washington", "regon", "california", "nevada"]
    mst = ['montana', 'Idaho', 'utah', 'Arizona', 'new mexico', 'colorado', 'wyoming']
    cst = ['dakota', 'nebraska', 'kansas', 'oklahoma', 'texas', 'minnesota', 'iowa', 'missouri',
    'arkansas', 'louisiana', 'wisconsin', 'illinois', 'tennessee', 'alabama', 'mississippi']
    est = ['Michigan', 'Indiana', 'virginia', 'Ohio', 'Pennsylvania', 'New York',
    'Vermont', 'Maine', 'New Hampshire', 'Massachusetts', 'Rhode Island',
    'Connecticut', 'New Jersey', 'Delaware', 'Maryland', 'District of Columbia',
    'georgia','florida', 'carolina', 'Kentucky']
    pst_abbreviations = ['CA', 'NV','OR', 'WA', ]
    mst_abbreviations = ['AZ', 'CO', 'ID', 'MT', 'NM', 'SD', 'UT', 'WY']
    cst_abbreviations =['AR', 'AL', 'FL', 'IL', 'IA', 'KS', 'KY', 'LA', 'MN','MS'
    'MO', 'NE', 'ND', 'OK', 'TX','WI']
    est_abbreviations = ['CT', 'DE', 'GA', 'IN', 'ME', 'MD', 'MA', 'MI', 'NH',
    'NJ', 'NY', 'NC', 'OH', 'PA', 'RI', 'SC', 'TN', 'VT', 'VA', 'DC', 'WV']
    for state in pst:
        if state.lower() in entry[0][1]:
            return ((entry[0][0], 'pst'), 1)
    for state in mst:
        if state.lower() in entry[0][1]:
            return ((entry[0][0], 'mst'), 1)
    for state in cst:
        if state.lower() in entry[0][1]:
            return ((entry[0][0], 'cst'), 1)
    for state in est:
        if state.lower() in entry[0][1]:
            return ((entry[0][0], 'est'), 1)
    for state in pst_abbreviations:
        if state in entry[0][1]:
            return ((entry[0][0], 'pst'), 1)
    for state in cst_abbreviations:
        if state in entry[0][1]:
            return ((entry[0][0], 'cst'), 1)
    for state in est_abbreviations:
        if state in entry[0][1]:
            return ((entry[0][0], 'est'), 1)
    for state in mst_abbreviations:
        if state in entry[0][1]:
            return ((entry[0][0], 'mst'), 1)
    return ('#', '#')

def query_4(sc, joined_data):
    data = joined_data.flatMap(lambda x: [((tag, (datetime.date(int(x[1][0].split("/")[2]), int(x[1][0].split("/")[0]), int(x[1][0].split("/")[1]))).strftime("%A")), 1) for tag in x[1][1]]).reduceByKey(lambda x, y: x + y)
    data = data.map(lambda x: query_4_restructure(x))
    data = data.groupByKey().mapValues(list)
    return data

def query_4_restructure(entry):
    day = entry[0][1]
    return (entry[0][0],(day, entry[1]))

def query_3(sc, joined_data):
    data = joined_data.flatMap(lambda x: [((tag, x[1][0]), 1) for tag in x[1][1]]).reduceByKey(lambda x, y: x + y)
    data = data.map(lambda x: query_3_restructure(x))
    data = data.groupByKey().mapValues(list)
    return data

def query_3_restructure(entry):
    day = entry[0][1]
    return (entry[0][0],(day, entry[1]))

def query_2(sc, joined_data, q1_results):
    data = joined_data.flatMap(lambda x: [((tag, x[1][0].split("/")[0]), 1) for tag in x[1][1]]).reduceByKey(lambda x, y: x + y)
    data = data.map(lambda x: query_2_restructure(x))
    data = data.groupByKey().mapValues(list)
    data = data.join(sc.parallelize(q1_results))
    return data

def query_2_restructure(entry):
    month = entry[0][1]
    return (entry[0][0],(month, entry[1]))

def query_1(sc, joined_data):
    data = joined_data.filter(lambda x: x[1][0].split("/")[2] == '2016') #CHECK YEAR
    data = data.flatMap(lambda x: [(tag, 1) for tag in x[1][1]]).reduceByKey(lambda x, y: x + y)
    return data

def get_user_pairs(entry):
    states = ["washington", "regon", "california", "nevada",
    'montana', 'Idaho', 'utah', 'Arizona', 'new mexico', 'colorado', 'wyoming',
    'dakota', 'nebraska', 'kansas', 'oklahoma', 'texas', 'minnesota', 'iowa', 'missouri',
    'arkansas', 'louisiana', 'wisconsin', 'illinois', 'tennessee', 'alabama', 'mississippi',
    'Michigan', 'Indiana', 'virginia', 'Ohio', 'Pennsylvania', 'New York',
    'Vermont', 'Maine', 'New Hampshire', 'Massachusetts', 'Rhode Island',
    'Connecticut', 'New Jersey', 'Delaware', 'Maryland', 'District of Columbia',
    'georgia','florida', 'carolina', 'Kentucky', 'hawaii', 'alaska']
    state_abbreviations = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI',
    'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT'
    'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN',
    'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    if (entry[0] == "id"):
        return ("#", "#")
    location = entry[1]
    uid = entry[0]
    if location != None:
        for state in states:
            if state in location:
                return (uid, location)
        for state in state_abbreviations:
            if state in location:
                return (uid, location)
    return ('#','#')

def get_post_pairs(entry):
    keywords = ['javascript', 'sql', 'java', 'c#', 'php', 'python', 'c++', 'js', 'ruby', 'objective-c'] # + c
    if (entry[0] == "id"):
        return ("#", "#")
    date = entry[1]
    date_vals = (date.split(" ")[0]).split("-")
    date = date_vals[1]+"/"+date_vals[2]+"/"+date_vals[0]
    tags = entry[2].split("|")
    correct_words = []
    for tag in tags:
        for word in keywords:
            if word in tag.lower():
                if (word == 'js'):
                    correct_words.append('javascript')
                else:
                    correct_words.append(word)
            if tag.lower().strip() == 'c':
                correct_words.append('c')
    if len(correct_words) == 0:
        return ("#", "#")
    uid = entry[3]
    return (uid, (date, correct_words))

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    # hdfs:///projects/group9/stackoverflow_users.csv
    parser.add_argument('input_user', help='File to load user data from')
    # hdfs:///projects/group9/posts_questions.csv
    parser.add_argument('input_post', help='File to load post data from')
    # hdfs:///projects/group9/stackoverflow_year.csv
    parser.add_argument('output_pie', help='File to save RDD to')
    # hdfs:///projects/group9/stackoverflow_month.csv
    parser.add_argument('output_month', help='File to save RDD to')
    # hdfs:///projects/group9/stackoverflow_day.csv
    parser.add_argument('output_date', help='File to save RDD to')
    # hdfs:///projects/group9/stackoverflow_day_of_week.csv
    parser.add_argument('output_day', help='File to save RDD to')
    # hdfs:///projects/group9/stackoverflow_timezone.csv
    parser.add_argument('output_timezone', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("stackoverflow_data")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    joined_data = find_engaging_reviews(sc, sqlContext, args.input_user, args.input_post)

    q1_results = query_1(sc, joined_data).collect()
    with open(args.output_pie, 'w') as csvfile:
        fieldnames = ['', 'javascript', 'sql', 'java', 'c#', 'php', 'python', 'c++', 'ruby', 'c', 'objective-c']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        row = {"": "Number Per Year"}
        for name in fieldnames:
            for res in q1_results:
                row.update({res[0]: res[1]})
        writer.writerow(row)

    q2_results = query_2(sc, joined_data, q1_results).collect()
    with open(args.output_month, 'w') as csvfile:
        fieldnames = ['Month', 'javascript', 'sql', 'java', 'c#', 'php', 'python', 'c++', 'ruby', 'c', 'objective-c']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for month in range(1,13):
            row = {}
            for name in fieldnames:
                if name == "Month":
                    row.update({"Month": month})
                for res in q2_results:
                    for months in res[1][0]:
                        if int(months[0]) == month:
                            row.update({res[0]: months[1]})
            writer.writerow(row)
        row = {'Month': "Total number in 2016"}
        for name in fieldnames:
            for res in q2_results:
                for months in res[1][0]:
                    if int(months[0]) == month:
                        row.update({res[0]: res[1][1]})
        writer.writerow(row)

    q3_results = query_3(sc, joined_data).collect()
    #args.output_date
    with open(args.output_date, 'w') as csvfile:
        fieldnames = ['Date', 'javascript', 'sql', 'java', 'c#', 'php', 'python', 'c++', 'ruby', 'c', 'objective-c']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        date_counter = datetime.date(2016, 1, 1)
        date_formatted = 'x'
        for num in range(0, 366):
            row = {}
            for name in fieldnames:
                if name == "Date":
                    date_formatted = (str(date_counter).split(' ')[0]).split('-')
                    date_formatted = date_formatted[1] + "/" + date_formatted[2] + "/" + date_formatted[0]
                    row.update({"Date": date_formatted})
                for res in q3_results:
                    for days in res[1]:
                        if days[0] == date_formatted:
                            row.update({res[0]: days[1]})
            date_counter += datetime.timedelta(days=1)
            writer.writerow(row)

    q4_results = query_4(sc, joined_data).collect()
    with open(args.output_day, 'w') as csvfile:
        fieldnames = ['Day of Week', 'javascript', 'sql', 'java', 'c#', 'php', 'python', 'c++', 'ruby', 'c', 'objective-c']
        days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for day in days_of_week:
            row = {}
            for name in fieldnames:
                if name == "Day of Week":
                    row.update({"Day of Week": day})
                for res in q4_results:
                    for days in res[1]:
                        if days[0] == day:
                            row.update({res[0]: days[1]})
            writer.writerow(row)

    q5_results = query_5(sc, joined_data).collect()
    with open(args.output_timezone, 'w') as csvfile:
        fieldnames = ['Timezone', 'javascript', 'sql', 'java', 'c#', 'php', 'python', 'c++', 'ruby', 'c', 'objective-c']
        timezones = ['pst', 'cst', 'mst', 'est']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for timezone in timezones:
            row = {}
            for name in fieldnames:
                if name == "Timezone":
                    row.update({"Timezone": timezone})
                for res in q5_results:
                    for timezones in res[1]:
                        if timezones[0] == timezone:
                            row.update({res[0]: timezones[1]})
            writer.writerow(row)
