from pathlib import Path

class WriteAheadLogger:
    def __init__(self, log_path: Path | str):
        
        if not isinstance(log_path, str) and not isinstance(log_path, Path):
            raise Exception('WTF IS THIS PATH MADE OF??')
        
        self.initialised = False
        
        if isinstance(log_path, str):
            self.log_path = Path(log_path)
            
    def write(operation, page, value, a):
        print('wwoenrownrownorno')
            
        