




def save_data(input_widgets):
    """
    Save data in `input_widgets` to an Excel file.

    """
    if not is_properly_filled(input_widgets):
        return

    column_names = []
    input_info = []
    get_info(input_widgets, column_names, input_info)
    if not os.path.isdir("Database"):
        os.mkdir("Database")
    try:
        os.chdir("Database")
        start_str = ""
        for entry in input_widgets[0][0]:
            start_str += entry.get() + "-"

        file_exists = True
        count = 0
        while file_exists:
            if os.path.isfile(start_str + f"{count}.xlsx"):
                count += 1
            else:
                name = start_str + f"{count}"
                file_exists = False

        with xlsxwriter.Workbook(f"{name}.xlsx") as workbook:
            worksheet = workbook.add_worksheet()
            worksheet.write_row(0, 0, column_names)
            worksheet.write_row(1, 0, input_info)

        create_messagebox(f"Жавобларингиз {name}.xlsx тарзида сақланди.", False)
        # util.log('success', f"Жавобларингиз {name}.xlsx тарзида сақланди.")
        os.chdir("..")
        clear_data(input_widgets)
    except Exception as error:
        create_messagebox(str(error))
        # util.log('error', str(error))


def merge_files():
    if not os.path.isdir("Database"):
        create_messagebox("Database папкаси мавжуд эмас.")
        return

    try:
        os.chdir("Database")
        extension = "xlsx"
        all_filenames = [i for i in glob('*.{}'.format(extension))]
        if not all_filenames:
            create_messagebox("Database папкаси бўш.")
            os.chdir("..")
            return
        elif os.path.exists("merged_file.xlsx"):
            create_messagebox("merged_file.xlsx папкада мавжуд.")
            os.chdir("..")
            return

        with xlsxwriter.Workbook("merged_file.xlsx") as workbook_full:
            worksheet = workbook_full.add_worksheet()
            current_row = 0
            for idx, file in enumerate(all_filenames):
                workbook = openpyxl.load_workbook(file)
                sheet = workbook.active
                max_col = sheet.max_column
                if idx == 0:
                    start_row = 1
                else:
                    start_row = 2
        
                for row in sheet.iter_rows(min_row=start_row, max_col=max_col, values_only=True):
                    worksheet.write_row(current_row, 0, row)
                    current_row += 1
        
                workbook.close()

        create_messagebox("Барча жавоблар бирлаштирилди.", False)
        os.chdir("..")

    except Exception as error:
        create_messagebox(str(error))
