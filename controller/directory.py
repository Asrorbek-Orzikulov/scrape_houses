import os
import pathlib


def main():
    home_path = pathlib.Path.home()
    os.chdir(home_path)
    if os.path.isdir('Desktop'):
        os.chdir('Desktop')
    else:
        os.mkdir('Desktop')
        os.chdir('Desktop')

    if os.path.isdir('Housing_Scrape'):
        os.chdir('Housing_Scrape')
    else:
        os.mkdir('Housing_Scrape')
        os.chdir('Housing_Scrape')
