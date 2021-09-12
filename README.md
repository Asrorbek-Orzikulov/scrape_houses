# Housing Data Scraper

Program written in Python that scrapes all announcements at www.olx.uz that are 
about the sale of apartments in Tashkent.

When you run the program, a folder called **Housing_Scrape** is created on 
Desktop. Also, you will see the following four buttons:

* Scrape Info – scrapes all announcements at www.olx.uz that are about the sale
of apartments in Tashkent. The button creates a **temporary_files** folder if it
does not exist in **Housing_Scrape**. There, scraped information will be saved as
88 pickle files. Each pickle file's name starts with the current date and 
represents a combination of `commission_type` (yes or no), `furnished_type`
(yes or no), `home_type` (yes or no), and one of 11 districts in Tashkent. This
structure was chosen to enable the program to continue scraping from where it 
stopped if it breaks during the execution. Although exception handling is done
inside every function in the **scraper.py**, the program can still break if a
computer loses internet connection or goes to sleep.

* Merge Districts – merges pickle files in the **temporary_files** folder of 
the form `current_date-commission_type-furnished_type-home_type-district.pkl`
into one pickle file with the name `current_date-merged.pkl`. The button creates
a **Database** folder if it does not exist and saves the merged pickle file there.

* Change Yesterday's Files – the `Merge Districts` button merges only those
pickle files that were created today. If some of your pickle files were created
yesterday, clicking on this button changes yesterday's date in their names 
to the current date.

* Make Excel – merges all pickle files in the **Database** folder of the form
`dd-mm-yyyy-merged.pkl` into one Excel file, dropping duplicates. The button
creates an Excel file with the name `current_date-merged.xlsx` and saves it 
in the **Database** folder. This function is useful because OLX displays
announcements that are made in the last two months. If you want to study the
changes in housing prices over time, run the program every month and generate
monthly data. After collecting data for several months, click on the
`Make Excel` button to merge all those monthly files.

To use the program, run the **main.py** and click on `Scrape Info`. When you
have all 88 files, click on `Merge Districts`. If some of your files were
generated yesterday and others today, click on `Change Yesterday's Files` 
before using the `Merge Districts` button. Finally, use `Make Excel` to merge 
all monthly data in the **Database** folder.
