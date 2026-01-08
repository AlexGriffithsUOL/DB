import struct

class PointerBlock:
    MAX_RIDS = 800
    def __init__(self, page_allocator, rids=None, overflow_page=None):
        """
        rids: list of (page_id, slot)
        overflow_page: page_id of the next overflow page (or None)
        """
        self.page_allocator = page_allocator
        self.rids = rids or []
        self.overflow_page = overflow_page

    # -------------------------------
    # Serialization / Deserialization
    # -------------------------------
    def save(self, page_id):
        """
        Serialize this pointer block to a page.
        """
        from struct import pack
        PAGE_SIZE = 4096
        data = bytearray(PAGE_SIZE)

        # Number of RIDs
        struct.pack_into("<H", data, 0, len(self.rids))
        offset = 2

        # Write all RIDs
        for pid, slot in self.rids:
            struct.pack_into("<HH", data, offset, pid, slot)
            offset += 4

        # Next overflow page
        struct.pack_into("<I", data, offset, self.overflow_page or 0)

        self.page_allocator.page_manager.write_page(page_id, data)

    @classmethod
    def load(cls, page_allocator, page_id):
        from struct import unpack_from
        data = page_allocator.page_manager.get_page(page_id).data

        num_rids = unpack_from("<H", data, 0)[0]
        offset = 2

        rids = [unpack_from("<HH", data, offset + i * 4) for i in range(num_rids)]
        offset += num_rids * 4

        overflow_page = unpack_from("<I", data, offset)[0] or None

        return cls(page_allocator, rids, overflow_page)

    def add_rid(self, rid):
        """Add a RID to this block. Returns overflow page if block is full."""
        # For simplicity, let's allow ~100 RIDs per block
        MAX_RIDS = 100
        if len(self.rids) < MAX_RIDS:
            self.rids.append(rid)
            return None
        else:
            # Allocate a new overflow page
            new_page = self.page_allocator.allocate_page()
            new_block = PointerBlock(self.page_allocator, rids=[rid])
            new_block.save(new_page)
            self.overflow_page = new_page
            return new_page

    def iter_rids(self):
        """Yield all RIDs in this block and follow overflow chain."""
        block = self
        while block:
            for rid in block.rids:
                yield rid
            if block.overflow_page:
                block = PointerBlock.load(self.page_allocator, block.overflow_page)
            else:
                break
