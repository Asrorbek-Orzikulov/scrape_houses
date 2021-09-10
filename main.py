import controller
import util
import tkinter as tk
from tkinter import ttk

#
# start of the GUI
#

root = tk.Tk()
root.title("Анкета Опроса Населения")
# root.iconbitmap("images/icon.ico")
root_width = root.winfo_screenwidth() - 15
root_height = root.winfo_screenheight() - 70
root.geometry("{0}x{1}+0+0".format(root_width, root_height))

# adding a scrollbar
main_frame = tk.Frame(root)
main_frame.pack(fill=tk.BOTH, expand=True)
canvas = tk.Canvas(main_frame)
canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=canvas.yview)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
canvas.configure(yscrollcommand=scrollbar.set)
canvas.bind("<Configure>",
            lambda event: canvas.configure(scrollregion=canvas.bbox("all")))

canvas.bind_all("<MouseWheel>",
                lambda event: canvas.yview_scroll(-1 * int((event.delta / 120)), "units"))

inside_frame = tk.Frame(canvas, borderwidth=10)
canvas.create_window((0, 0), window=inside_frame, anchor="nw")

#
# Title of questionnaire
#

head_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
head_frame.pack(fill=tk.BOTH, expand=True)
head_frame.grid_columnconfigure(0, weight=1)
head_frame.grid_rowconfigure(0, weight=1)
label_head = tk.Label(head_frame, text="Ўзбекистон Республикасида  ижтимоий-сиёсий вазиятни ўрганиш (2021 июль)")
label_head.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_head.config(font=("helvetica", 16), fg="dark blue")

#
# info section
#

info_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
info_frame.pack(fill=tk.BOTH, expand=True)
label_info = tk.Label(info_frame, text="Сўров иштирокчиларининг ижтимоий-демографик хусусиятлари")
label_info.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_info.config(font=("helvetica", 14), fg="green")

# Question 0
label_0_row = 1
texts_0 = ["Интервьюер IDси: ", "Маҳалла IDси: ", "Анкета IDси: "]
question_0 = []
for idx, text_0 in enumerate(texts_0):
    row_0 = label_0_row+idx
    label_0 = tk.Label(info_frame, text=text_0)
    label_0.grid(row=row_0, column=0, padx=10, pady=10, sticky="e")
    label_0.config(font=("helvetica", 12))
    entry_0 = tk.Entry(info_frame, validate="key")
    entry_0.configure(validatecommand=(entry_0.register(is_digit),'%P','%d'))
    entry_0.grid(row=row_0, column=1, padx=10, pady=10)
    question_0.append(entry_0)

# Question 1
label_1_row = 4
text_1 = "1. Исмингиз нима?"
num_options_1 = 1
question_1 = inputting_questions(info_frame, label_1_row,
                                 num_options_1, text_1, False)[0]

# Question 2
label_2_row = 6
text_2 = "2. Сиз вилоятда доимий яшайсизми ва рўйхатдан ўтганмисиз?"
options_2 = ["Ҳа", "Йўқ"]
question_2 = radio_button_question(info_frame, label_2_row,
                                    options_2, text=text_2)

# Question 3
label_3_row = 9
text_3 = "3. Сиз вилоятнинг қайси тумани(шаҳри)да яшайсиз?"
num_options_3 = 1
question_3 = inputting_questions(info_frame, label_3_row,
                                 num_options_3, text_3, False)[0]

# Question 4
label_4_row = 11
text_4 = "4. Шаҳар ёки қишлоқ тугмасини танланг."
options_4 = ["Шаҳар", "Қишлоқ", "Жавоб беришдан бош тортиш"]
question_4 = radio_button_question(info_frame, label_4_row, options_4, text_4)

# Question 5
label_5_row = 15
text_5 = "5. Респондентнинг жинсини киритинг."
options_5 = ["Эркак", "Аёл"]
question_5 = radio_button_question(info_frame, label_5_row, options_5, text_5)

# Question 6
label_6_row = 18
label_6 = tk.Label(info_frame, text="6. Респондентнинг ёшини киритинг.")
label_6.grid(row=label_6_row, column=0, padx=10, pady=30, columnspan=3)
label_6.config(font=("helvetica", 12))
question_6 = tk.Entry(info_frame, validate="key")
question_6.configure(validatecommand=(question_6.register(is_digit),'%P','%d'))
question_6.grid(row=label_6_row+1, column=0, columnspan=3, padx=10, pady=10)

#
# welfare section
#

welfare_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
welfare_frame.pack(fill=tk.BOTH, expand=True)
label_welfare = tk.Label(welfare_frame, text="Ижтимоий фаровонлик диагностикаси. Кайфиятлар")
label_welfare.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_welfare.config(font=("helvetica", 14), fg="green")

# Question 7
label_7_row = 1
text_7 = "7. Мамлакатдаги умумий вазиятдан (ҳолатдан) қониқиш даражангизни 7 баллик шкалада баҳоланг."
options_7 = [1, 2, 3, 4, 5, 6, 7]
question_7 = radio_button_question(welfare_frame, label_7_row,
                                     options_7, text_7)

# Question 8
label_8_row = 9
text_8 = "8. Сизнингча, мамлакатдаги умумий вазият яхшиланмоқдами, ёмонлашмоқдами ёки деярли ўзгармаяптими?"
question_8 = radio_button_question(
    welfare_frame, label_8_row, ["Яхшиланмоқда", "Ёмонлашмоқда", "Деярли ўзгармаяпти", "Жавоб беришга қийналаман"],
    text=text_8)

# Question 9
label_9_row = 14
text_9 = "9. Вилоятингиздаги умумий вазиятдан (ҳолатдан) қониқиш даражангизни 7 баллик шкалада баҳоланг."
question_9 = radio_button_question(welfare_frame, label_9_row, [1, 2, 3, 4, 5, 6, 7],
                                   text=text_9)

# Question 10
label_10_row = 22
text_10 = "10. Сизнингча, вилоятдаги умумий вазият яхшиланмоқда, ёмонлашмоқда ёки деярли ўзгармаяптими?"
question_10 = radio_button_question(welfare_frame, label_10_row, ["Яхшиланмоқда", "Ёмонлашмоқда", "Деярли ўзгармаяпти", "Жавоб беришга қийналаман"],
                                   text=text_10)

# Question 11
label_11_row = 27
text_11 = "11. Маҳаллангиздаги умумий вазиятдан (ҳолатдан) қониқиш даражангизни 7 баллик шкалада баҳоланг."
question_11 = radio_button_question(welfare_frame, label_11_row, [1, 2, 3, 4, 5, 6, 7],
                            text=text_11)

# Question 12
label_12_row = 35
text_12 = "12. Сизнингча, сиз яшаётган маҳаллада умумий вазият яхшиланмоқдами, ёмонлашмоқдами ёки ҳеч нарса ўзгармаяптими?"
question_12 = radio_button_question(welfare_frame, label_12_row, ["Яхшиланмоқда", "Ёмонлашмоқда", "Деярли ўзгармаяпти", "Жавоб беришга қийналаман"],
                            text=text_12)

# Question 13
label_13_row = 40
text_13 = "13. Сизнингча, атрофингиздаги одамлар ҳозир қандай кайфиятда: кўтаринки, хотиржам ёки тушкун (хавотирли)?"
question_13 = radio_button_question(welfare_frame, label_13_row, ["Кўтаринки", "Хотиржам", "Тушкун, хавотирли", "жавоб беришга қийналаман"],
                            text=text_13)

# Question 14
label_14_row = 45
text_14 = "14. “Мен Ўзбекистон иқтисодиёти ривожланишига ишонаман” Мазкур фикрга"
question_14 = radio_button_question(welfare_frame, label_14_row, ["Тўлиқ қўшиламан", "Қисман қўшиламан", "Қисман қўшилмайман", "Умуман қўшилмайман"],
                            text=text_14)

# Question 15
label_15_row = 50
text_15 = "15. “Мамлакатимизда олиб борилаётган ислоҳотлар тўғри йўлда кетмоқда”  Мазкур фикрга"
question_15 = radio_button_question(welfare_frame, label_15_row, ["Тўлиқ қўшиламан", "Қисман қўшиламан", "Қисман қўшилмайман", "Умуман қўшилмайман"],
                            text=text_15)

# Question 16
label_16_row = 55
text_16 = "16. Давлат томонидан қуйида келтирилган соҳалардан қай бирида амалга оширилаётган ишларни маъқуллайсиз?"
options_16 = [
    "Таълим тизими",
    "Соғлиқни сақлаш тизими",
    "Ҳудудлардаги инфратузилмани (газ, свет, йўл ва ҳ.к.) яхшилаш",
    "Коррупция қарши кураш",
    "Аҳолини уй-жой билан таъминлаш",
    "Камбағаликка қарши кураш",
    "Тадбиркорларни қўллаб-қувватлаш",
    "Халқ билан очиқ мулоқотнинг кучайиши",
    "Ҳеч бирини"
    ]
question_16 = checkbox_question(welfare_frame, label_16_row, options=options_16,
                                text=text_16, has_other=True)

#
# prezident section
#

prezident_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
prezident_frame.pack(fill=tk.BOTH, expand=True)
label_prezident = tk.Label(prezident_frame, text="Президент")
label_prezident.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_prezident.config(font=("helvetica", 14), fg="green")

# Question 17
label_17_row = 1
text_17 = "17. Сизнинг фикрингизча, ҳукумат фуқаролар билан очиқ мулоқотда бўлмоқда ва уларнинг муаммоларига ўз вақтида жавоб қайтармоқда?"
options_17 = ["Тўлиқ қўшиламан", "Қисман қўшиламан", "Қисман қўшилмайман", "Умуман қўшилмайман"]
question_17 = radio_button_question(prezident_frame, label_17_row,
                                    options_17, text_17)

# Question 18
label_18_row = 6
text_18 = "18. Илтимос, сиз фикрига қулоқ соладиган, ҳурмат қиладиган сиёсатчилар, жамоат арбобларининг исмларини айтинг"
num_options_18 = 5
question_18 = inputting_questions(prezident_frame, label_18_row, num_options_18, text_18, True)

# table for Questions 19-26
table_frame = tk.LabelFrame(prezident_frame, borderwidth=5, highlightthickness=0, padx=10, pady=10)
table_frame.grid(row=13, column=0, padx=10, pady=10, columnspan=8)
options_19_26 = [1, 2, 3, 4]

# Question 19
label_19_row = 2
text_19 = "19. Абдулла Арипов, Ўзбекистон Республикаси Бош вазири"
question_19 = radio_button_question(table_frame, label_19_row, options_19_26,
                            text=text_19)

# Question 20
label_20_row = 7
text_20 = "20. Танзила Нарбаева, Ўзбекистон Республикаси Олий Мажлиси Сенати раиси"
question_20 = radio_button_question(table_frame, label_20_row, options_19_26,
                            text=text_20)

# Question 21
label_21_row = 12
text_21 = "21. Нурдинжон Исмоилов, Ўзбекистон Республикаси Олий Мажлиси Қонунчилик палатасининг спикери"
question_21 = radio_button_question(table_frame, label_21_row, options_19_26,
                            text=text_21)

# Question 22
label_22_row = 17
text_22 = "22. Актам Хаитов, Ўзбекистон Либерал-демократик партияси етакчиси"
question_22 = radio_button_question(table_frame, label_22_row, options_19_26,
                            text=text_22)

# Question 23
label_23_row = 22
text_23 = "23. Алишер Қодиров, Ўзбекистон «Миллий тикланиш» демократик партияси етакчиси"
question_23 = radio_button_question(table_frame, label_23_row, options_19_26,
                            text=text_23)

# Question 24
label_24_row = 27
text_24 = "24. Баҳром Абдухалимов, Ўзбекистон «Адолат»  демократик партияси етакчиси "
question_24 = radio_button_question(table_frame, label_24_row, options_19_26,
                            text=text_24)

# Question 25
label_25_row = 32
text_25 = "25. Улуғбек Иноятов, Ўзбекистон Халқ демократик партияси етакчиси"
question_25 = radio_button_question(table_frame, label_25_row, options_19_26,
                            text=text_25)

# Question 26
label_26_row = 37
text_26 = "26. Нарзулло Обломуродов, Ўзбекистон Экология партияси етакчиси"
question_26 = radio_button_question(table_frame, label_26_row, options_19_26,
                            text=text_26)

# Question 27
label_27_row = 14
text_27 = "27. Президент Шавкат Мирзиёев ўз лавозимида фикрингиз бўйича қандай фаолият кўрсатаётганлигини баҳоланг."
options_27 = [1, 2, 3, 4, 5, 6, 7]
question_27 = radio_button_question(prezident_frame, label_27_row, options_27,
                            text=text_27)

# Question 28
label_28_row = 22
text_28 = "28. Ўтган икки ой мобайнида Шавкат Мирзиёевга Ўзбекистон Президенти сифатида муносабатингиз ўзгардими?"
options_28 = ["Анча яхшиланди", "Озроқ яхшиланди", "Озроқ ёмонлашди", "Анча ёмонлашди", "Ҳеч нарса ўзгармади", "Жавоб беришга қийналаман"]
question_28 = radio_button_question(prezident_frame, label_28_row,
                                    options_28, text=text_28)

# Question 29
label_29_row = 29
text_29 = "29. Шавкат Мирзиёевга ишонасизми ёки ишонмайсизми?"
options_29 = ["Тўлиқ ишонаман", "Ишонаман", "Ишонмайман", "Тўлиқ ишонмайман", "Жавоб беришга қийналаман"]
question_29 = radio_button_question(prezident_frame, label_29_row, options=options_29,
                           text=text_29)

# Question 30
label_30_row = 35
text_30 = "30. Илтимос, Шавкат Мирзиёевнинг Президентлик даврида эришган асосий ютуқларини номлаб беринг."
options_30 = [
    "1. Конвертация очилганлиги (валюта сиёсати)",
    "2. Ташқи савдонинг эркинлашуви",
    "3. Солиқ сиёсати",
    "4. Сўз эркинлигининг кучайиши",
    "5. Яқин қўшнилар билан алоқанинг мустаҳкамланиши",
    "6. Дин ва эътиқод эркинлигининг кучайиши",
    "7. Камбағалликка қарши кураш ишлари",
    "8. Соғлиқни сақлаш тизимидаги ислоҳотлар",
    "9. Янги иш ўринларининг яратилиши",
    "10. Мигрантлар учун шароитларнинг яхшиланиши",
    "11. Халқ билан мулоқотнинг кучайиши",
    "12. Ёшларни қўллаб-қувватлашнинг кучайтирилиши",
    "13. Тадбиркорликни қўллаб-қувватлаш ишлари",
    "14. Аҳолини уй-жой билан таъминлаш",
    "15. Давлат хизматлари сифатининг ортиши",
    "16. Мактабгача таълим тизимидаги ишлар",
    "17. Олий таълим тизимидаги ислоҳотлар",
    "18. Мактаб таълимидаги ўзгаришлар",
    "19. Ҳеч қандай ютуғи йўқ",
    "20. Жавоб беришга қийналаман"
    ]
question_30 = checkbox_question(prezident_frame, label_30_row, options_30,
                               text_30, True)

# Question 31
label_31_row = 58
text_31 = "31. Илтимос, Шавкат Мирзиёевнинг Президентлик давридаги асосий камчиликларини номлаб беринг"
options_31 = [
    "1. Коррупцияга қарши кураш ишлари",
    "2. Аҳолини тоза ичимлик суви билан таъминлаш",
    "3. Аҳолини электрэнергия билан таъминлаш",
    "4. Аҳолини табиий газ билан таъминлаш",
    "5. Етарли янги иш ўринларини яратиш",
    "6. Нарх-навонинг кўтарилиши",
    "7. Аҳолига ижтимоий моддий ёрдам кўрсатиш",
    "8. Ташқи қарзнинг кўпайиши",
    "9. Айрим соҳаларда монополиянинг сақланиб қолиниши",
    "10. Ҳеч қандай камчилиги йўқ",
    "11. Жавоб беришга қийналаман"
    ]
question_31 = checkbox_question(prezident_frame, label_31_row, options_31,
                               text_31, True)

# Question 32
label_32_row = 72
text_32 = "32. Сизнингча, Шавкат Мирзиёев мамлакатдаги вазиятни яхши томонга ўзгартира оладими ёки йўқми?"
options_32 = [
    "Аниқ ўзгартира олади",
    "Ўзгартириши мумкин",
    "Ўзгартириши қийин",
    "Аниқ ўзгартира олмайди",
    "Жавоб беришга қийналаман"
    ]
question_32 = radio_button_question(
    prezident_frame, label_32_row, options=options_32, text=text_32)

# Question 33
label_33_row = 79
text_33 = "33. Сизнингча, Ўзбекистон Республикаси Президенти Шавкат Мирзиёев биринчи навбатда қандай вазифаларни ҳал этиши керак?"
num_options_33 = 3
question_33 = inputting_questions(prezident_frame, label_33_row, num_options_33, text_33, True)

# Question 34
label_34_row = 84
text_34 = "34. Сизнингча, мамлакатга янги Президент керакми ёки ҳозирги Президент қолгани маъқулми?"
options_34 = ["Янги Президент керак", "Амалдаги Президентнинг қолгани яхши" , "Менга фарқи йўқ", "Жавоб беришга қийналаман"]
question_34 = radio_button_question(prezident_frame, label_34_row, options_34, text_34)

#
# major section
#

major_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
major_frame.pack(fill=tk.BOTH, expand=True)
label_major = tk.Label(major_frame, text="Ҳокимлар") ###########################
label_major.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_major.config(font=("helvetica", 14), fg="green")

# Question 35
label_35_row = 1
text_35 = "35. Айтингчи, Сиз вилоят ҳокимига ишонасизми ёки ишонмайсизми?"
options_35 = [
    "Тўлиқ ишонаман",
    "Қисман ишонаман",
    "Қисман ишонмайман",
    "Умуман ишонмайман",
    "Вилоят ҳокимини танимайман (37 саволга ўтинг)",
    "Жавоб беришга қийналаман"]
question_35 = radio_button_question(major_frame, label_35_row, options_35, text_35)

# Question 36
label_36_row = 10
text_36 = "36. Сизнингча, вилоят/республика хокими сифатида қандай ишлаяпти?"
options_36 = [1, 2, 3, 4, 5, 6, 7]
question_36 = radio_button_question(major_frame, label_36_row, options_36, text_36)

# Question 37
label_37_row = 18
text_37 = "37. Сизнингча вилоят/республикага янги ҳоким керакми ёки амалдаги ҳоким қолгани яхшими?"
options_37 = ["Янги ҳоким керак","Амалдаги қолгани яхши","Менга фарқи йўқ","Жавоб беришга қийналаман"]
question_37 = radio_button_question(major_frame, label_37_row, options_37, text_37)

# Question 38
label_38_row = 23
text_38 = "38. Туманингизнинг ҳокими. Сиз туманингиз ҳокимимга ишонасизми ёки ишонмайсизми?"
options_38 = [
    "Тўлиқ ишонаман",
    "Ишонаман",
    "Ишонмайман",
    "Тўлиқ ишонмайман",
    "Туман (шахар) ҳокимини танимайман (40 саволга ўтинг)",
    "Жавоб беришга қийналаман"]
question_38 = radio_button_question(major_frame, label_38_row, options_38, text_38)

# Question 39
label_39_row = 31
text_39 = "39. Сизнингча, туманингиз ҳокими ўз лавозимида қандай ишламоқда?"
options_39 = [1, 2, 3, 4, 5, 6, 7]
question_39 = radio_button_question(major_frame, label_39_row, options_39, text_39)

# Question 40
label_40_row = 39
text_40 = "40. Сизнингча туманга янги ҳоким керакми ёки амалдаги ҳоким қолгани яхшими? "
options_40 = ["Янги ҳоким керак","Амалдаги қолгани яхши","Менга фарқи йўқ","Жавоб беришга қийналаман"]
question_40 = radio_button_question(major_frame, label_40_row, options_40, text_40)

# Question 41
label_41_row = 44
text_41 = "41. Вилоят ҳокимини ҳозиргидек тайинлаган маъқулми ёки уни овоз бериш орқали сайлаганми?"
options_41 = ["Тайинланса яхши","Сайланса яхши","Менга фарқи йўқ","Жавоб беришга қийналаман"]
question_41 = radio_button_question(major_frame, label_41_row, options_41, text_41)

# Question 42
label_42_row = 49
text_42 = "42. Туман ҳокимини ҳозиргидек тайинлаган маъқулми ёки уни овоз бериш орқали сайлаганми?"
options_42 = ["Тайинланса яхши","Сайланса яхши","Менга фарқи йўқ","Жавоб беришга қийналаман"]
question_42 = radio_button_question(major_frame, label_42_row, options_42, text_42)

#
# parliament section
#

parliament_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
parliament_frame.pack(fill=tk.BOTH, expand=True)
label_parliament = tk.Label(parliament_frame, text="Парламент ва сиёсий партиялар")
label_parliament.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_parliament.config(font=("helvetica", 14), fg="green")

# Question 43
label_43_row = 1
text_43 = "43. Ўзбекистон Республикаси Олий Мажлис Қонунчилик палатасининг амалдаги таркибининг фаолиятини қандай баҳолайсиз?"
options_43 = [1, 2, 3, 4, 5, 6, 7]
question_43 = radio_button_question(parliament_frame, label_43_row, options_43, text_43)

# Question 44
label_44_row = 9
text_44 = "44. Агар Олий Мажлис Қонунчилик палатаси сайловлари кейинги якшанба куни бўлиб ўтса, сиз қайси партияга овоз берган бўлардингиз?"
options_44 = [
    "1. Ўзбекистон Либерал-демократик партияси",
    "2. “Миллий тикланиш” демократик партияси",
    "3. Ўзбекистон Халқ демократик партияси",
    "4. “Адолат” социал-демократик партияси",
    "5. Экология партияси",
    "6. Ҳеч қайси учун/ҳаммасига қарши",
    "7. Бюллетенни йиртаман",
    "8.Сайловга бормайман",
    "9. Жавоб беришга қийналаман",
    "10. Рад этиш"
    ]
question_44 = radio_button_question(parliament_frame, label_44_row, options_44, text_44, True)


# Question 45
label_45_row = 22
text_45 = "45. Нега айнан шу партияга овоз берган бўлардингиз?"
num_options_45 = 2
question_45 = inputting_questions(parliament_frame, label_45_row, 2, text_45, True)

# Question 46
label_46_row = 26
text_46 = "46. Агар танлаган партиянгиз бюллетенларда бўлмаса, қайси партияга овоз берасиз?"
options_46 = [
    "1. Ўзбекистон Либерал-демократик партияси",
    "2. “Миллий тикланиш” демократик партияси",
    "3. Ўзбекистон Халқ демократик партияси",
    "4. “Адолат” социал-демократик партияси",
    "5. Экология партияси",
    "6. Ҳеч қайси учун / ҳаммасига қарши",
    "7. Бюллетенни йиртаман",
    "8. Сайловга бормайман",
    "9. Жавоб беришга қийналаман",
    "10. Рад этиш"
    ]
question_46 = radio_button_question(parliament_frame, label_46_row,
                                    options_46, text_46, True)

# Question 47
label_47_row = 39
text_47 = "47. Ҳар қандай шароитда ҳам қайси партияларга овоз бермайсиз?"
options_47 = [
    "1. Ўзбекистон Либерал-демократик партияси",
    "2. “Миллий тикланиш” демократик партияси",
    "3. Ўзбекистон Халқ демократик партияси",
    "4. “Адолат” социал-демократик партияси",
    "5. Экология партияси",
    "6. Ҳеч қайси учун / ҳаммасига қарши",
    "7. Бюллетенни бузиш",
    "8. Сайловга бормайман",
    "9. Жавоб беришга қийналаман",
    "10. Рад этиш"
    ]
question_47 = checkbox_question(parliament_frame, label_47_row,
                                options_47, text_47, True)

#
# presidential elections section
#

elections_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
elections_frame.pack(fill=tk.BOTH, expand=True)
label_parliament = tk.Label(elections_frame, text="Республика Президентлигига сайлов")
label_parliament.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_parliament.config(font=("helvetica", 14), fg="green")

# Question 48
label_48_row = 1
text_48 = "48. Ўзбекистон Республикаси президенти сайлови қачон ўтказилишини биласизми?"
options_48 = [
    "2021 й. 24 октябрь",
    "2021 й. октябрь",
    "билмайман/жавоб беришга қийналаман",
    "бошқа сана"
    ]
question_48 = radio_button_question(elections_frame, label_48_row, options_48, text_48)

# Question 49
label_49_row = 6
text_49 = "49. Одатда, сайловга борасизми?"
options_49 = [
    "1. Мен барча сайловларга бораман",
    "2. Баъзан сайловларга бораман",
    "3. Бормайман",
    "4. Жавоб беришга қийналаман",
]
question_49 = radio_button_question(elections_frame, label_49_row, options_49, text_49)

# Question 50
label_50_row = 11
text_50 = "50. 2021 йилги Ўзбекистон Республикаси Президенти сайловларида қатнаша оласизми ёки қатнаша олмайсизми?"
options_50 = [
    "1. Ҳозирча аниқ эмас, сайловга яқин қарор қиламан",
    "2. Албатта қатнашаман",
    "3. Йўқ",
    "4. Аниқ айта олмайман",
    "5. Жавоб беришга қийналаман",
]
question_50 = radio_button_question(elections_frame, label_50_row, options_50, text_50)

# Question 51
label_51_row = 17
text_51 = "51. Қайси партиядан президентликка номзодга овоз берган бўлардингиз? Ёки партия муҳим эмасми?"
options_51 = [
    "1. Ўзбекистон Либерал-демократик партияси",
    "2. “Миллий тикланиш” демократик партияси",
    "3. Ўзбекистон Халқ демократик партияси",
    "4. “Адолат” социал-демократик партияси",
    "5. Экология партияси",
    "6. Партиясиз",
    "7. Партия муҳим эмас",
    "8. Жавоб беришга қийналаман",
]
question_51 = radio_button_question(elections_frame, label_51_row,
                                    options_51, text_51, True)

#
# ICT section
#

ict_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
ict_frame.pack(fill=tk.BOTH, expand=True)
label_parliament = tk.Label(ict_frame, text="Ахборот манбалари")
label_parliament.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_parliament.config(font=("helvetica", 14), fg="green")

# Question 52
label_52_row = 1
text_52 = "52. Мамлакат ҳаётидаги қизиқарли воқеалар ҳақида асосан қаердан маълумот оласиз?"
options_52 = [
    "1. Телевидение",
    "2. Босма нашрлар (Газета, журналлар)",
    "3. Радио",
    "4. Интернет",
    "5. Машхур блогерлар",
    "6. Танишлар, дўстлар",
    "7. Маҳаллий ва ҳудудий янгиликларга қизиқмайман",
    "8. Жавоб беришга қийналаман"
]

question_52 = checkbox_question(ict_frame, label_52_row, options=options_52,
                                text=text_52, has_other=True)

# Question 53
label_53_row = 12
text_53 = "53. Ўзингиз учун қайси ижтимоий тармоқни асосий деб ҳисоблайсиз?"
options_53 = [
    "1. Тик-ток",
    "2. Телеграмм",
    "3. Фейсбук ",
    "4. Инстаграм ",
    "5. Ютьюб ",
    "6. ЛинкдИн ",
    "7. Твиттер ",
    "8. Ижтимоий тармоқларда аккаунтим йўқ/ижтимоий тармоқларга кирмайман ",
    "9. Жавоб беришга қийналаман"
]
question_53 = radio_button_question(ict_frame, label_53_row,
                                    options_53, text_53, True)

# Question 54
label_54_row = 24
text_54 = "54. Сиз яшаб турган ҳудуддаги энг асосий муаммони кўрсатинг."
options_54 = [
    "1. Газ таъминоти билан боғлиқ муаммо",
    "2. Ичимлик суви билан боғлиқ муаммо",
    "3. Электр энергия таъминоти билан боғлиқ муаммо",
    "4. Ички йўллар сифати билан боғлиқ муаммо ",
    "5. Иситиш тизими билан боғлиқ муаммо",
    "6. Жамоат транспорти билан боғлиқ муаммо",
    "7. Канализация билан боғлиқ муаммо",
    "8. Чиқиндилар билан боғлиқ муаммо",
    "9. Коррупция билан боғлиқ муаммо",
    "10. Уй-жой ширкатлари билан боғлиқ муаммо",
    "11. Етарли иш ўринлари мавжуд эмас",
    "12. Уй-жой билан таъминлаш муаммоси",
    "13. Мактабгача таълим муассаси (боғча) билан боғлиқ муаммо",
    "14. Маҳалла билан боғлиқ муаммо",
    "15. Интернет билан боғлиқ муаммо",
    "16. Телефон алоқаси билан боғлиқ муаммо",
    "17. Муаммо йўқ",
    "18. Жавоб беришга қийналаман",
]
question_54 = checkbox_question(ict_frame, label_54_row, options=options_54,
                               text=text_54, has_other=True)

#
# financials section
#

financials_frame = tk.LabelFrame(inside_frame, borderwidth=10, highlightthickness=0, padx=10, pady=10)
financials_frame.pack(fill=tk.BOTH, expand=True)
label_parliament = tk.Label(financials_frame, text="Молиявий аҳвол")
label_parliament.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
label_parliament.config(font=("helvetica", 14), fg="green")

# Question 55
label_55_row = 1
text_55 = "55. Маълумотингиз қандай?"
options_55 = [
    "1. Тугалланмаган ўрта, ўрта (мактаб, лицей) таълим",
    "2. Ўрта махсус таълим (коллеж)",
    "3. Олий (бакалавр, магистр)",
]

question_55 = radio_button_question(financials_frame, label_55_row,
                                    options_55, text_55)

# Question 56
label_56_row = 5
text_56 = "56. Ҳозирги пайтда асосий фаолиятингиз қандай?"
options_56 = [
    "1. Давлат ташкилотида ишлайман",
    "2. Хусусий секторда ишлайман",
    "3. Якка тартибдаги тадбиркор, ўз-ўзини иш билан банд қилиш",
    "4. Ўқувчи, талаба,",
    "5. Ишламайдиган пенсионер",
    "6. Вақтинча ишсиз, ишсиз",
    "7. Уй бекаси, туғриқ таътилидаман",
    "8. Жавоб беришни рад этди"
]

question_56 = radio_button_question(financials_frame, label_56_row,
                                    options_56, text_56)

# Question 57
label_57_row = 14
text_57 = "57. Қайси соҳада ишлайсиз?"
options_57 = [
    "1. Саноат соҳаси ",
    "2. Қишлоқ хўжалиги",
    "3. Савдо, аҳолига хизмат кўрсатиш",
    "4. Коммунал хўжалиги",
    "5. Қурилиш ",
    "6. Транспорт ",
    "7. Алоқа",
    "8. Давлат ёки маҳаллий бошқарув",
    "9. Ички ишлар, ҳарбий, ФВВ",
    "10. Таълим",
    "11. Соғлиқни сақлаш",
    "12. Маданият",
    "13. Молия ",
    "14. Оммавий ахборот воситалари",
    "15. Жавоб беришни рад этди",
]
question_57 = radio_button_question(financials_frame, label_57_row, options_57, text_57, True)

# Question 58
label_58_row = 32
text_58 = "58. Ҳозирги пайтда асосий фаолиятингиз қандай?"
options_58 = [
    "1. Озиқ-овқат учун пул етмайди. Биз зўрға кун кечирамиз",
    "2. Озиқ-овқат учун етарли, коммунал хизматлар тўловлари учун етмайди ",
    "3. Озиқ-овқат ва коммунал хизматлар тўловлари учун етарли, ноозиқ овқат товарлар \n"\
    "(кийим-кечак, гигиена воситалари) учун етмайди",
    "4. Озиқ-овқат, коммунал хизматлар тўловлари ва ноозиқ овқат товарлар учун етарли, \n"\
    "бироқ узоқ муддатли товарлар (музлатгич, телевизор, чангютгич, кондиционер ва ҳ.к.) учун етмайди",
    "5. Озиқ-овқат, коммунал хизматлар тўловлари, ноозиқ овқат товарлар, узоқ муддатли товарлар \n"\
    "(музлатгич, телевизор, чангютгич, кондиционер ва ҳ.к.) учун етарли, аммо автомобиль, уй сотиб олишга етмайди",
    "6. Ҳамма нарсага етади",
    "7. Жавоб беришни рад этди"
]
question_58 = radio_button_question(financials_frame, label_58_row,
                                    options_58, text_58)

# Question 59
label_59_row = 41
text_59 = "59. Боғланиш учун телефон рақамингизни киритинг (камида 2 та рақамни киритиш)"
label_59 = tk.Label(financials_frame, text=text_59)
label_59.grid(row=label_59_row, column=0, padx=10, pady=10, columnspan=3)
label_59.config(font=("helvetica", 12))
question_59 = [tk.Entry(financials_frame, validate="key"),
               tk.Entry(financials_frame, validate="key")]
for idx, entry in enumerate(question_59):
    entry.configure(validatecommand=(entry.register(is_digit), '%P', '%d'))
    entry.grid(row=label_59_row+idx+1, column=0, columnspan=3, padx=10, pady=10)

# aggregation of all responses
question_widgets = [
    (question_0, "visible"),
    (question_1, "visible"),
    (question_2, "visible"),
    (question_3, "visible"),
    (question_4, "visible"),
    (question_5, "visible"),
    (question_6, "visible"),
    (question_7, "visible"),
    (question_8, "visible"),
    (question_9, "visible"),
    (question_10, "visible"),
    (question_11, "visible"),
    (question_12, "visible"),
    (question_13, "visible"),
    (question_14, "visible"),
    (question_15, "visible"),
    (question_16, "hidden"),
    (question_17, "visible"),
    (question_18, "visible"),
    (question_19, "visible"),
    (question_20, "visible"),
    (question_21, "visible"),
    (question_22, "visible"),
    (question_23, "visible"),
    (question_24, "visible"),
    (question_25, "visible"),
    (question_26, "visible"),
    (question_27, "visible"),
    (question_28, "visible"),
    (question_29, "visible"),
    (question_30, "hidden"),
    (question_31, "hidden"),
    (question_32, "visible"),
    (question_33, "visible"),
    (question_34, "visible"),
    (question_35, "visible"),
    (question_36, "visible"),
    (question_37, "visible"),
    (question_38, "visible"),
    (question_39, "visible"),
    (question_40, "visible"),
    (question_41, "visible"),
    (question_42, "visible"),
    (question_43, "visible"),
    (question_44, "hidden"),
    (question_45, "visible"),
    (question_46, "hidden"),
    (question_47, "hidden"),
    (question_48, "visible"),
    (question_49, "visible"),
    (question_50, "visible"),
    (question_51, "hidden"),
    (question_52, "hidden"),
    (question_53, "hidden"),
    (question_54, "hidden"),
    (question_55, "visible"),
    (question_56, "visible"),
    (question_57, "hidden"),
    (question_58, "visible"),
    (question_59, "visible"),
    ]

# buttons at the bottom
bottom_frame = tk.LabelFrame(root, borderwidth=4, highlightthickness=4, padx=10, pady=0)
bottom_frame.pack(side=tk.BOTTOM, fill=tk.X)
button_exit = tk.Button(bottom_frame, text="  Exit Program  ",
                        command=root.quit, relief=tk.RAISED)
button_exit.pack(side=tk.LEFT, pady=5, padx=20)
button_make_onefile = tk.Button(bottom_frame, text="    Merge Files    ",
                                command=merge_files, relief=tk.RAISED)
button_make_onefile.pack(side = tk.LEFT, pady=5, padx=20)
button_save = tk.Button(bottom_frame, text="Save Responses", relief=tk.RAISED,
                        command=lambda: save_data(question_widgets))
button_save.pack(side=tk.RIGHT, pady=5, padx=20)
button_clear = tk.Button(bottom_frame, text="Clear Responses", relief=tk.RAISED,
                         command=lambda: clear_data(question_widgets))
button_clear.pack(side=tk.RIGHT, pady=5, padx=20)

util.main()
controller.main()
root.mainloop()
