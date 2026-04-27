from src.parser.tokens import TOKENS as tokens

def t_IDENTIFIER(t):
    r'[A-Za-z_][A-Za-z0-9_]*'
    # Convert SQL keywords to uppercase to simplify parsing
    t.type = t.value.upper() if t.value.upper() in tokens else 'IDENTIFIER'
    return t

def t_NUMBER(t):
    r'\d+(\.\d+)?'
    t.value = float(t.value) if '.' in t.value else int(t.value)
    return t

def t_STRING(t):
    r"'([^']*)'"
    t.value = t.value[1:-1]  # Remove quotes
    return t

def t_COMMENT(t):
    r'(--[^\n]*|/\*[\s\S]*?\*/)'
    pass  # ignore comments

def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

def t_error(t):
    print(f"Illegal character '{t.value[0]}' at line {t.lineno}")
    t.lexer.skip(1)

t_ignore = ' \t\n'