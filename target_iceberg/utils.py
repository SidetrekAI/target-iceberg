def singer_records_to_list(records):
    pylist = []
    
    for r in records:
        record = r["record"]
        record["time_extracted"] = r["time_extracted"]
        pylist.append(record)
        
    return pylist