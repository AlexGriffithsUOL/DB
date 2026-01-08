class FakeBtree:
    max_data = 4096
    
    def __init__(self):
        self.data = bytearray(b''.ljust(self.max_data, b'\x00'))