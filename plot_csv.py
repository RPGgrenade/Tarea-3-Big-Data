import matplotlib.pyplot as plt
import csv
import numpy as np

info = dict()


def plot_data():
    for category in sorted(info.items()):
        ind = np.arange(5)
        width = 0.5

        fig, ax = plt.subplots()

        ax.set_title(category[0])
        ax.set_ylabel('Number')
        ax.set_xlabel('Stars')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('1', '2', '3', '4', '5'))

        scores = (category[1][1], category[1][2], category[1][3], category[1][4], category[1][5])

        rects = ax.bar(ind, scores, width, color='r')
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height,
                    '%d' % int(height),
                    ha='center', va='bottom')

        plt.show()


def setup_data(file):
    reader = csv.reader(file)
    for line in reader:
        if len(line) > 0:
            category = line[1]
            stars = int(line[2])
            if category not in info.keys():
                info[category] = dict()
                info[category][1] = 0
                info[category][2] = 0
                info[category][3] = 0
                info[category][4] = 0
                info[category][5] = 0
                info[category][stars] += 1
            else:
                if stars not in info[category].keys():
                    info[category][stars] = 1
                else:
                    info[category][stars] += 1

file_10000 = open('stars_per_category_10000.csv', 'r')
setup_data(file_10000)
plot_data()

file_30000 = open('stars_per_category_30000.csv', 'r')
setup_data(file_30000)
plot_data()