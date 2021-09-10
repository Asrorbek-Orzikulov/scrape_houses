from colorama import Fore, Back, Style


def log(state, logs):
    if state.lower() == "warn":
        print(Fore.YELLOW + Style.NORMAL + "[WARNING]" + Style.RESET_ALL + " " + logs)
    elif state.lower() == "info":
        print(Fore.LIGHTBLUE_EX + Style.NORMAL + "[INFO]" + Style.RESET_ALL + " " + logs)
    elif state.lower() == "error":
        print(Fore.RED + Style.NORMAL + "[ERROR]" + Style.RESET_ALL + " " + logs)
    elif state.lower() == "success":
        print(Fore.GREEN + Style.NORMAL + "[SUCCESS]" + Style.RESET_ALL + " " + logs)
    else:
        print(Back.RED + Back.WHITE + Style.NORMAL + "[MODE NOT SPECIFIED]" + Style.RESET_ALL)
    pass
