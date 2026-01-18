def print_unbuffered(capsys, msg: str):
    with capsys.disabled():
        print(msg)

