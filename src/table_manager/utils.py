def filter_by_tx_snapshot( records, tx_snapshot=None, locations=None):
        if not isinstance(records, list):
            records = [records]
        
        filtered_records = []
        filtered_locations = []
        for i, record in enumerate(records):
            if tx_snapshot is None:
                filtered_records.append(record)
                
            else:
                if record["i$tx_created"] <= tx_snapshot and (record["i$tx_deleted"] == 0 or record["i$tx_deleted"] > tx_snapshot):
                    filtered_records.append(record)
                    
                    if locations is not None:
                        filtered_locations.append(locations[i])
        
        if locations is not None:
            return filtered_records, filtered_locations

        return filtered_records
    
def single_filter_by_tx_snapshot(record: dict, tx_snapshot_id: int = None):
    if tx_snapshot_id is None:
        return record
    
    if record["i$tx_created"] <= tx_snapshot_id and (record["i$tx_deleted"] == 0 or record["i$tx_deleted"] > tx_snapshot_id):
        return record