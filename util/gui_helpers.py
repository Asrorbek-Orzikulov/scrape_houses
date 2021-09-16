from os import path, chdir, mkdir, getcwd, listdir, rename
from re import compile
from pandas import DataFrame, concat, read_pickle
from tkinter import messagebox


def create_messagebox(text, is_error=True):
    """
    Display a message box with the given `text`.

    Parameters
    ----------
    text : str
        text message to be displayed.
    is_error : bool
        if True, an error message box will be shown.
        Otherwise, displays an information message box.

    Returns
    -------
    None.

    """
    if is_error:
        messagebox.showerror(title='Saving Request', message=text)
    else:
        messagebox.showinfo(title='Saving Request', message=text)


def find_files(pattern):
    """
    Find all filenames in the current folder that match the pattern.

    Parameters
    ----------
    pattern : str
        regex pattern to use to match the filenames.

    Returns
    -------
    filenames : list
        list of filenames that match the pattern.

    """
    filenames_pattern = compile(pattern)
    filenames = list(filter(filenames_pattern.match, listdir('./')))
    return filenames


def merge_district_pickles(date):
    """
    Merge pickle files in the `temporary_files` folder of the form
    `date-commission_type-furnished_type-home_type-district.pkl`
    into one pickle file with the name `date-merged.pkl`.

    Creates a `Database` folder if it does not exist and saves the
    merged pickle file there.

    Parameters
    ----------
    date : str
        date of the form `dd-mm-yyyy`.

    Returns
    -------
    None.

    """
    if not path.isdir('temporary_files'):
        create_messagebox('temporary_files folder does not exist.')
        return

    chdir('temporary_files')
    filenames_pattern = f'{date}-' + r'(yes|no)-.*\.pkl$'
    all_filenames = find_files(filenames_pattern)
    if not all_filenames:
        create_messagebox(f'Pickle files do not exist in {getcwd()}')
        chdir('..')
        return

    create_messagebox(f'Found {len(all_filenames)} files to merge.', False)
    merged_data = concat([read_pickle(file) for file in all_filenames])
    chdir('..')

    if not path.isdir('Database'):
        mkdir('Database')
    chdir('Database')
    merged_filename = f'{date}-merged.pkl'
    merged_data.to_pickle(merged_filename)
    create_messagebox(f'{merged_filename} has been created.', False)
    chdir('..')


def merge_month_pickles():
    """
    Merge all pickle files in the current folder of the form
    `dd-mm-yyyy-merged.pkl` into one pandas DataFrame, dropping
    duplicated rows.

    Returns
    -------
    pandas DataFrame.

    """
    filenames_pattern = r'\d{2}-\d{2}-\d{4}-merged\.pkl$'
    all_filenames = find_files(filenames_pattern)
    if not all_filenames:
        return DataFrame()

    create_messagebox(f'Found {len(all_filenames)} files to merge.', False)
    merged_data = concat([read_pickle(file) for file in all_filenames])
    merged_data.drop_duplicates(
        subset=['price', 'num_rooms', 'area', 'home_type', 'district', 'post_text'],
        inplace=True
    )

    # data pre-processing
    filter_conditions = '(area >= 30) and (area <= 310) and (price_m2 >= 300)'\
        ' and (apart_floor <= home_floor) and (num_rooms <= 8)'
    merged_data = merged_data.query(filter_conditions)
    merged_data[['day', 'month', 'year']] = merged_data.date.str.split(
        pat="-", expand=True)
    return merged_data


def create_excel(date):
    """
    Merge all pickle files in the `Database` folder of the form
    `dd-mm-yyyy-merged.pkl` into one Excel file, dropping duplicates.

    Creates an Excel file with the name `date-merged.xlsx` and
    saves it in the `Database` folder.

    Parameters
    ----------
    date : str
        date of the form `dd-mm-yyyy`.

    Returns
    -------
    None.

    """
    if not path.isdir('Database'):
        create_messagebox('Database folder does not exist.')
        return

    chdir('Database')
    df = merge_month_pickles()
    if df.empty:
        create_messagebox(f'Pickle files do not exist in {getcwd()}')
        chdir('..')
        return

    filename = f'{date}-merged.xlsx'
    df.to_excel(filename, index=False, encoding='utf-8')
    create_messagebox(f'{filename} has been created.', False)
    chdir('..')


def update_yesterday(yesterday, today):
    """
    Rename all filenames in the `temporary_files` folder of the form
    `yesterday-commission_type-furnished_type-home_type-district.pkl`
    as `today-commission_type-furnished_type-home_type-district.pkl`.

    Parameters
    ----------
    yesterday: str
        yesterday's date of the form `dd-mm-yyyy`.
    today: str
        today's date of the form `dd-mm-yyyy`.

    Returns
    -------
    None.

    """
    if not path.isdir('temporary_files'):
        create_messagebox('temporary_files folder does not exist.')
        return

    chdir('temporary_files')
    yesterday_pattern = f'{yesterday}-' + r'(yes|no)-.*\.pkl$'
    yesterday_files = find_files(yesterday_pattern)
    num_renamed = 0
    for filename in yesterday_files:
        new_filename = filename.replace(f'{yesterday}', f'{today}')
        if not path.isfile(new_filename):
            rename(filename, new_filename)
            num_renamed += 1

    create_messagebox(f'{num_renamed} files have been renamed.', False)
    chdir('..')


def on_enter(event):
    """Change the background color of a widget to blue."""
    event.widget['background'] = '#33E6FF'


def on_leave(event):
    """Change the background color of a widget to green."""
    event.widget['background'] = '#3DC70D'
