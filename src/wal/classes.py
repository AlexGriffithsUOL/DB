import os
from pathlib import Path
from src.config import ENDIAN_TYPE
import json

class WriteAheadLogger:
    TX_ID_LENGTH = 64
    
    def _create_wal(self):
        if not self.log_path.parent.exists():
            os.makedirs(self.log_path.parent.absolute())
        
        with open(self.log_path, 'wb') as file:
            file.flush()
            file.close()
            
    def _load_wal(self):
        with open(self.log_path, 'rb') as file:
            self.data = file.read()
            file.close()
    
    def __init__(self, log_path: Path | str):
        
        if not isinstance(log_path, str) and not isinstance(log_path, Path):
            raise Exception('WTF IS THIS PATH MADE OF??')
        
        self.initialised = False
        
        if isinstance(log_path, str):
            self.log_path = Path(log_path)
            
        if not self.log_path.exists():
            self._create_wal()
            
    def _get_zero_value(self):
        return int.to_bytes(0, 64, ENDIAN_TYPE)
            
    def write(self, tx_id, tx_action_type: str, table: str, rid, old_data = None, new_data = None, index: str = None, key = None):
        #TXID | ActionType | Table | RID | OldData | NewData | Index | Key
        tx_id_raw = int.to_bytes(tx_id, self.TX_ID_LENGTH, ENDIAN_TYPE, signed=False)
        tx_action_type_raw = tx_action_type.encode('utf-8')
        tx_action_type_length = int.to_bytes(len(tx_action_type_raw), 64, ENDIAN_TYPE, signed=False)
        
        if len(tx_action_type_raw) > 32:
            raise Exception('Action type too lare')
        
        table_raw = table.encode('utf-8')
        table_length = int.to_bytes(len(table_raw), 64, ENDIAN_TYPE, signed=False)
        
        if len(table_raw) > 256:
            raise Exception('Table too large')
        
        page_id = rid[0]
        slot_id = rid[1]
        
        page_id_raw = int.to_bytes(page_id, 64, ENDIAN_TYPE, signed=False)
        slot_id_raw = int.to_bytes(slot_id, 64, ENDIAN_TYPE, signed=False)
        
        if old_data is not None:
            old_data_raw = json.dumps(old_data).encode('utf-8')
            old_data_length = int.to_bytes(len(old_data_raw), 64, ENDIAN_TYPE, signed=False)
        else:
            old_data_length = self._get_zero_value()
        
        if new_data is not None:
            new_data_raw = json.dumps(new_data).encode('utf-8')
            new_data_length = int.to_bytes(len(new_data_raw), 64, ENDIAN_TYPE, signed=False)
        else:
            new_data_length = self._get_zero_value()
        
        if index is not None:
            index_raw = index.encode('utf-8')
            index_length = int.to_bytes(len(index_raw), 64, ENDIAN_TYPE, signed=False)
        else:
            index_length = self._get_zero_value()
        
        if key is not None:
            if isinstance(key, str):
                key_raw = key.encode('utf-8')
            
            if isinstance(key, int):
                key_raw = int.to_bytes(key, 64, ENDIAN_TYPE, signed=False)
        
            key_length = int.to_bytes(len(key_raw), 64, ENDIAN_TYPE, signed=False)
        
        else:
            key_length = self._get_zero_value()


        empty = bytearray()
        empty += tx_id_raw
        empty += tx_action_type_length
        empty += tx_action_type_raw
        empty += table_length
        empty += table_raw
        empty += page_id_raw
        empty += slot_id_raw
        
        empty += old_data_length
        if old_data:
            empty += old_data_raw
            
        empty += new_data_length
        if new_data:
            empty += new_data_raw
        
        empty += index_length
        if index:
            empty += index_raw
            
        empty += key_length
        if key is not None:
            empty += key_raw
        
        with open(self.log_path, 'ab') as file:
            file.write(empty)
            file.flush()
            file.close()
    
    def read_64(self, offset):
        start = offset
        end = start + 64
        total = 64
        length = int.from_bytes(self.data[start:end], ENDIAN_TYPE, signed=False)
            
        data_start = end
        data_end = data_start + length
        total += length
            
        if length > 0:
            raw_data = self.data[data_start:data_end]
        else:
            raw_data = None
        
        return raw_data, total
        
    
    @property
    def content(self):
        self._load_wal()
        
        read_lines = []
        
        current_idx = 0
        while current_idx < len(self.data):
            total = 0
            row = []
            
            tx_id_start = current_idx
            tx_id_end = current_idx + 64
            total += 64
            tx_id = int.from_bytes(self.data[tx_id_start:tx_id_end], ENDIAN_TYPE, signed=False)
            row.append(tx_id)
            
            tx_at_start = tx_id_end
            tx_at_end = tx_at_start + 64
            total += 64
            tx_action_type_length = int.from_bytes(self.data[tx_at_start:tx_at_end], ENDIAN_TYPE, signed=False)
            
            tx_action_type_start = tx_at_end
            tx_action_type_end = tx_action_type_start + tx_action_type_length
            total += tx_action_type_length
            
            tx_action_type = self.data[tx_action_type_start: tx_action_type_end].decode('utf-8')
            row.append(tx_action_type)
            
            table_len_start = tx_action_type_end
            table_len_end = table_len_start + 64
            total += 64
            table_length = int.from_bytes(self.data[table_len_start:table_len_end], ENDIAN_TYPE, signed=False)
            
            table_start = table_len_end
            table_end = table_start + table_length
            total += table_length
            
            table = self.data[table_start:table_end].decode('utf-8')
            row.append(table)
            
            page_start = table_end
            page_end = page_start + 64
            page = int.from_bytes(self.data[page_start:page_end], ENDIAN_TYPE, signed=False)
            total += 64
            row.append(page)
            
            slot_start = page_end
            slot_end = slot_start + 64
            slot = int.from_bytes(self.data[slot_start:slot_end], ENDIAN_TYPE, signed=False)
            total += 64
            row.append(slot)
            
            raw_old_data, total_add = self.read_64(slot_end)
            old_data = raw_old_data.decode('utf-8') if raw_old_data is not None else raw_old_data
            row.append(old_data)
            total += total_add
            new_end = total
            
            raw_new_data, total_add2 = self.read_64(new_end)
            new_data = raw_new_data.decode('utf-8') if raw_new_data is not None else raw_new_data
            row.append(new_data)
            total += total_add2
            new_end = total
            
            raw_index, total_add3 = self.read_64(new_end)
            index = raw_index.decode('utf-8') if raw_index is not None else raw_index
            row.append(index)
            total += total_add3
            new_end = total
            
            raw_key, total_add4 = self.read_64(new_end)
            key = int.from_bytes(raw_key, ENDIAN_TYPE, signed=False) if raw_key is not None else raw_key
            row.append(key)
            total += total_add4
            new_end = total
            
            current_idx += total
            read_lines.append(row)
        
        return read_lines