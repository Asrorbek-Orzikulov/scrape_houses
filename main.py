import tkinter as tk

import controller
import util
from scraper import ScraperOLX


def on_enter(e):
    e.widget['background'] = 'blue'

def on_leave(e):
    e.widget['background'] = 'green'


scraper = ScraperOLX()
root = tk.Tk()
root.title("Scraping Apartment Prices")
root_width = root.winfo_screenwidth() - 15
root_height = root.winfo_screenheight() - 70
root.geometry("400x210")
main_frame = tk.Frame(root)
main_frame.pack(fill=tk.BOTH, expand=True, pady=10)

button_scrape = tk.Button(main_frame, text="Scrape Info", width=30,
                          command=scraper.scrape_everything, relief=tk.RAISED,
                          bg='green', fg='black')
button_merge_district = tk.Button(main_frame, text="Merge Districts", width=30,
                                  command=scraper.merge_district_pickles,
                                  bg='green', fg='black')
button_merge_all = tk.Button(main_frame, text="Merge All Files", width=30,
                             command=scraper.merge_month_pickles,
                             bg='green', fg='black')
button_make_excel = tk.Button(main_frame, text="Make Excel", width=30,
                              command=scraper.create_excel,
                              bg='green', fg='black')

buttons = [
    button_scrape, button_merge_district, button_merge_all, button_make_excel
]
for button in buttons:
    button.pack(side=tk.TOP, pady=10, padx=20)
    button.bind("<Enter>", on_enter)
    button.bind("<Leave>", on_leave)

util.main()
controller.main()
root.mainloop()
