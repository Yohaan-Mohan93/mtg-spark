import os

def create_directory(path_to_file):
    if not os.path.exists(path_to_file):
        os.makedirs(path_to_file)

    return os.path.exists(path_to_file)