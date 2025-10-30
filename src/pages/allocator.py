from .pages import PageManager
from src.config import ENDIAN_TYPE

class BitWriter:
    def read_bits(self, data):
        for byte in data:
            for i in range(7, -1, -1):
                bit = (byte >> i) & 1
                
        return bit
    
    def write_bit(self, offset, data):
        byte_floor = offset // 8
        byte_offset = offset % 8
        byte_ceiling = byte_floor + 1
        byte = data[byte_floor:byte_ceiling]
        byte[0] |= (1 << byte_offset)
        data[byte_floor:byte_ceiling] = byte
        
    def write_at_locations(self, offset_list, data):
        for offset in offset_list:
            self.write_bit(offset, data)

class PageAllocator:

    BITMAP_AVAILABLE = 0
    BITMAP_FULL = 1
    RESERVED_BITMAP_NEXT_PTR_SIZE = 4
    RESERVED_BITMAP_NEXT_PTR_BIT_SIZE = 32
    INITIAL_RESERVED_BITMAP_PAGE = 1
    fresh_superblock = False
    

    def __init__(self, page_manager: PageManager = None):
        
        if page_manager is None:
            page_manager = PageManager()
        
        self.page_manager: PageManager = page_manager
        self.bitmap_start_id = 0
        self.bitmap_page_ids = []
        
        self.init()

    def _get_superblock_page(self):
        return self.page_manager.get_page(0)
    
    @property
    def initialised(self):
        superblock_page = self._get_superblock_page()
        return not all(b == 0 for b in superblock_page.data)

    def _load_superblock(self):
        superblock = self._get_superblock_page()
        
        magic = superblock.data[0:4].decode('utf-8')
        
        if magic != 'PYDB':
            raise Exception("Invalid database file")
        
        version = superblock.data[4:6].decode('utf-8')
        print('PYDB Version: ' + version)
        
        # bitmap_dir_start_size = 4
        # bitmap_dir_start_offset = 6
        # bitmap_dir_start_end = bitmap_dir_start_offset + bitmap_dir_start_size
        # bitmap_dir_start = int.from_bytes(superblock.data[bitmap_dir_start_offset:bitmap_dir_start_end], ENDIAN_TYPE)
        self.bitmap_start_id = self.INITIAL_RESERVED_BITMAP_PAGE
        
        self.sys_table_pid = int.from_bytes(superblock.data[10:14], ENDIAN_TYPE)
        self.sys_columns_pid = int.from_bytes(superblock.data[14:18], ENDIAN_TYPE)
        self.table_counter = int.from_bytes(superblock.data[18:22], ENDIAN_TYPE)
        self.catalog_table_id_counter = int.from_bytes(superblock.data[18:34], ENDIAN_TYPE)
        
        
    def _bootstrap(self):
        self.page_manager.alloc_page(0)
        superblock_page = self._get_superblock_page()
        superblock_page.data[0:4] = b'PYDB'
        superblock_page.data[4:6] = b'00'
        superblock_page.data[6:10] = int(self.INITIAL_RESERVED_BITMAP_PAGE).to_bytes(4, ENDIAN_TYPE) 
        superblock_page.data[10:14] = int(2).to_bytes(4, ENDIAN_TYPE) # system_tables index
        superblock_page.data[14:18] = int(3).to_bytes(4, ENDIAN_TYPE) # system_columns index
        superblock_page.data[18:34] = int(5).to_bytes(16, ENDIAN_TYPE) # system_columns index
        
        bitmap_page =self._initialise_bitmap_page()
        bw = BitWriter()
        bitmap_reserve_locations = [0, 1]
        bitmap_reserve_locations = [x + self.RESERVED_BITMAP_NEXT_PTR_BIT_SIZE for x in bitmap_reserve_locations]
        bw.write_at_locations(bitmap_reserve_locations, bitmap_page.data)
        self.page_manager.write_page(bitmap_page.id, bitmap_page.data)
        self.page_manager.flush()
        self.bitmap_start_id = self.INITIAL_RESERVED_BITMAP_PAGE
        
    def _initialise_bitmap_page(self, linking: bool = False):
        data = self._initialise_bitmap_data()
        self.page_manager.alloc_next_raw_page()
        page = self.page_manager.get_latest_page()
        self.page_manager.write_page(page.id, data)
        
        # link so we can build initial tree
        if linking:
            prev_bitmap_id = self.bitmap_page_ids[-1]
            prev_bitmap_page = self.page_manager.get_page(prev_bitmap_id)
            prev_bitmap_page.data[0:4] = int(page.id).to_bytes(4, ENDIAN_TYPE)
            self.page_manager.write_page(prev_bitmap_id, prev_bitmap_page.data)
        
        self.bitmap_page_ids.append(page.id)
        self.page_manager.flush()
        return self.page_manager.get_page(page.id)
    
    def _initialise_bitmap_data(self):
        reserved_bitmap_pointer = bytearray(b''.ljust(self.RESERVED_BITMAP_NEXT_PTR_SIZE, b'\x00'))
        initial_zeros = bytearray([0b00000000] * (self.page_manager.file_manager.config.page_size_kb - 4))
        return bytearray(reserved_bitmap_pointer+ initial_zeros)

    def init(self):
        if self.initialised:
            self._load_superblock()
        else:
            self._bootstrap()
            self._load_superblock()
            self.fresh_superblock = True
        
        self.bitmap_page_ids = self.build_bitmap_page_ids(self.bitmap_start_id, [])

    def build_bitmap_page_ids(self, bitmap_page_id, bitmap_id_list: list):
        bitmap_page = self.page_manager.get_page(bitmap_page_id)
        next_page_id = int.from_bytes(bitmap_page.data[0:4], ENDIAN_TYPE)
        
        bitmap_id_list.append(bitmap_page_id)
        if next_page_id == 0:
            return bitmap_id_list
        
        return self.build_bitmap_page_ids(next_page_id, bitmap_id_list)

    def _find_next_free_id(self):
        """
        Finds the index of the first free (0) bit in the bitmap.
        Returns the bit index or -1 if none are free.
        """
        for page_count, page_id in enumerate(self.bitmap_page_ids):
            page = self.page_manager.get_page(page_id)
        
            for byte_index, byte in enumerate(page.data[self.RESERVED_BITMAP_NEXT_PTR_SIZE:]):
                
                if byte != 0xFF:
                    for bit in range(8):
                        if not (byte >> bit) & 1:
                            return (byte_index * 8 + bit) + (page_count * self.bitmap_page_potential_ids)
        return None
    
    @property
    def bitmap_page_potential_ids(self):
        return (self.page_manager.file_manager.config.page_size_kb - self.RESERVED_BITMAP_NEXT_PTR_SIZE) * 8
    
    def _get_bitmap_page_and_offset(self, page_id):
        """
        Makes it way easier to just grab a bitmap page, looks at the bitmap list, and indexes (only works because the list is linked and will ALWAYs be in order)
        """
        bitmap_page_index, offset = self._get_page_flips_and_offset(page_id)
        bitmap_page_id = self.bitmap_page_ids[bitmap_page_index]
        bitmap_page = self.page_manager.get_page(bitmap_page_id)
        return bitmap_page, offset
    
    def _mark_page_id(self, page_id):
        bitmap_page, offset = self._get_bitmap_page_and_offset(page_id)
        bw = BitWriter()
        bw.write_bit(offset, bitmap_page.data)
        self.page_manager.write_page(bitmap_page.id, bitmap_page.data)

    def allocate_page(self):
        next_page_id = self._find_next_free_id()
        
        if next_page_id is None:
            self._initialise_bitmap_page(linking=True)
            return self.allocate_page()
        
        self._mark_page_id(next_page_id)
        self.page_manager.alloc_page(next_page_id)
        return next_page_id
    
    def get_page(self, id = None):
        if id is None:
            id = self.allocate_page()
        return self.page_manager.get_page(id)
        
    def _get_page_flips_and_offset(self, page_id):
        ids_per_page = (self.page_manager.file_manager.config.page_size_kb - self.RESERVED_BITMAP_NEXT_PTR_SIZE) * 8
        bottom = page_id  // ids_per_page
        offset = (page_id % ids_per_page)
        return bottom, offset + self.RESERVED_BITMAP_NEXT_PTR_BIT_SIZE

    def free_page(self, page_id):
        bitmap_page, offset = self._get_bitmap_page_and_offset(page_id)
        bw = BitWriter()
        bw.free_bit(offset, bitmap_page.data)
        self.page_manager.write_page(bitmap_page.id, bitmap_page.data)
        
