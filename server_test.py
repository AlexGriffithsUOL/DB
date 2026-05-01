from src.server.classes import Server

s = Server()
request = {'action': 'select', 'from': 'test', 'where':1000}
print(s.process_request(request))