from src.parser.tokeniser import Tokeniser
from src.parser.parser import Parser

script = 'select s.*\n' \
    'from student s\n' \
    'left outer join school sc on sc.school_id = s.school_id\n'\
    'where s.student_id >= 2\n'\
    'and s.school_id = 1;'

script2 = "select * from student where name = 'stantonbury' and school_id = 1;"

t = Tokeniser(script2)

tokens = t.tokenise()

p = Parser(tokens)
ast = p.parse_select()


print(tokens)
print()
print(ast)