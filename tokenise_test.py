from src.parser.tokeniser import Tokeniser

sql = 'select * from test where test_id = 1000;'

tk = Tokeniser()
tokens = tk.tokenise(sql)

for t in tokens:
    print(t)

