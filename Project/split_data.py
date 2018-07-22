import random
import os

def partition(buckets):
    prob = random.random()
    for name, percent in buckets.items():
        if prob <= percent:
            return name
        prob -= percent

if __name__ == '__main__':
    buckets = {
        'train':0.7,
        'validate':0.15,
        'test':0.15,
    }
    input_path = './input'
    paths = {
        'train':input_path + "/train.csv",
        'validate':input_path + "/validate.csv",
        'test':input_path + "/test.csv",
    }
    directory = os.fsencode(input_path)
    for f in os.listdir(directory):
        filename = os.fsdecode(f)
        with open(filename, 'r') as data_file:
            for line in data_file:
                name = partition(buckets)
                with open(paths[name], 'a') as bucketed_file:
                    bucketed_file.write(line)

