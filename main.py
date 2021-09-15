from tkinter import Tk, Frame, Button, BOTH, TOP

import controller
import util
from scraper import ScraperOLX


scraper = ScraperOLX()
root = Tk()
root.title("Scraping Apartment Prices")
root_width = root.winfo_screenwidth() - 15
root_height = root.winfo_screenheight() - 70
root.geometry("400x230")
main_frame = Frame(root)
main_frame.pack(fill=BOTH, expand=True, pady=10)

button_scrape = Button(
    main_frame, text="Scrape Info", width=30, bg='#3DC70D', fg='black',
    command=scraper.scrape_everything)
button_change_yesterday = Button(
    main_frame, text="Change Yesterday's Files", width=30, bg='#3DC70D', fg='black',
    command=lambda: util.update_yesterday(scraper.yesterday, scraper.today))
button_merge_district = Button(
    main_frame, text="Merge Districts", width=30, bg='#3DC70D', fg='black',
    command=lambda: util.merge_district_pickles(scraper.today))
button_make_excel = Button(
    main_frame, text="Make Excel", width=30, bg='#3DC70D', fg='black',
    command=lambda: util.create_excel(scraper.today))

buttons = [button_scrape, button_change_yesterday,
           button_merge_district, button_make_excel]
for button in buttons:
    button.pack(side=TOP, pady=10, padx=20)
    button.bind("<Enter>", util.on_enter)
    button.bind("<Leave>", util.on_leave)
    button.configure(font=("Arial", 12))

util.main()
controller.main()
root.mainloop()
