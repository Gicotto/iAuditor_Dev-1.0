import os

def get_environment():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    if dir_path.find('_dev') != -1:
        return '_dev'
    if dir_path.find('_stage') != -1:
        return '_stage'
    if dir_path.find('_prod') != -1:
        return '_prod'
    else:
        return dir_path

env = get_environment()
print(f"environment is {env}")