import os
import getpass

def f_env_config():
    """
    The environment configuration is a dictionary of path directory.
    """
    os.environ['DIR_DATASET'] = 'C:\\Users\\eudes\\Documents\\github\\dataset'

f_env_config()
