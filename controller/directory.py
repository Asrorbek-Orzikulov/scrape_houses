from os import path, chdir, mkdir
from pathlib import Path


def main():
    home_path = Path.home()
    chdir(home_path)
    if path.isdir('Desktop'):
        chdir('Desktop')
    else:
        mkdir('Desktop')
        chdir('Desktop')

    if path.isdir('Housing_Scrape'):
        chdir('Housing_Scrape')
    else:
        mkdir('Housing_Scrape')
        chdir('Housing_Scrape')
