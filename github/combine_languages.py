# Combine all language data into a csv

f = open("languages.csv", "r").read()

repos = {}

f = f.strip().split("\n")
f = f[1:]

for row in f:
    repo, language = row.split(",")
    if repo in repos:
        repos[repo].append(language)
    else:
        repos[repo] = [language]

new = open("languages_combined.csv", "w")
new.write("repo,languages\n")
for repo in repos:
    new.write("{},\"{}\"\n".format(repo, ",".join(repos[repo])))
