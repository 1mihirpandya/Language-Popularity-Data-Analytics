import glob
import argparse
import os, sys
import re
WORD = re.compile(r"[^(|,|'|\s|)]+")
#WORD = re.compile(r"[\w+-/]+")
import subprocess


if __name__ == '__main__':

    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input_folder', help='File to load Amazon review data from')
    parser.add_argument('output_file', help='File to load Amazon review data from')
    parser.add_argument('final_output', help='File to load Amazon review data from')


    args = parser.parse_args()
    #files = glob.glob(args.input_folder)
    dirs = os.listdir(args.input_folder)

    # p = subprocess.Popen("hdfs dfs -ls /user/renukan2/year_2016_gh_FUCK |  awk '{print $8}",
    #                      shell=True,
    #                      stdout=subprocess.PIPE,
    #                      stderr=subprocess.STDOUT)
    fop = open(args.output_file, 'w')
    for file_name in dirs:
        #print(file_name)
        f = open(args.input_folder+file_name, 'r')
        for line in f:
            fop.write(line)

        f.close()

    fop.close()

    ftog = open(args.output_file, 'r')
    final = open(args.final_output, 'w')
    #langs = {"Python", "Java", "JavaScript", "Ruby", "SQL", "C#", "C++", "nodejs", "PHP", "C", "objective-c"}
    langs = ["JavaScript", "SQL", "Java", "C#" , "PHP", "Python", "C++", "C", "nodejs", "Ruby", "objective-c"]

    row = {}

    for line in ftog:
        line_l = WORD.findall(line)

        if line_l[0] in row:
            key_l = row.get(line_l[0])
            key_l[8] = 0
            key_l[langs.index(line_l[1])] = int(line_l[2])
            #key_l[langs.index(line_l[2])] = int(line_l[4])
            row.update({line_l[0]: key_l})

        else:
            key_l = [0]*11
            key_l[8] = 0
            key_l[langs.index(line_l[1])] = int(line_l[2])
            #key_l[langs.index(line_l[1])] = int(line_l[4])
            row.update({line_l[0]: key_l})

    print(row)

    final.write("Timezone,JavaScript,SQL,Java,C#,PHP,Python,C++,C,nodejs,Ruby,objective-c\n")
    for key in row.keys():
        out_str = str(key)
        key_l = row.get(key)
        for val in key_l:
            out_str += ","+str(val)
        final.write(out_str+"\n")

    final.close()

    ftog.close()







