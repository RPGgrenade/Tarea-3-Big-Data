import csv

def make_csv(output_name, dictionary):
    with open(output_name + '.csv', 'w') as f:
        w = csv.writer(f)
        for key,value in dictionary.items():
            w.writerow([key, value[0], value[1]])
