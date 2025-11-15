SECTIONS = [
    (1,5,"I"),(6,14,"II"),(15,15,"III"),(16,24,"IV"),(25,27,"V"),
    (28,38,"VI"),(39,40,"VII"),(41,43,"VIII"),(44,46,"IX"),(47,49,"X"),
    (50,63,"XI"),(64,67,"XII"),(68,70,"XIII"),(71,83,"XIV"),
    (84,85,"XV"),(86,89,"XVI"),(90,92,"XVII"),(93,94,"XVIII"),
    (95,95,"XIX"),(96,96,"XX"),(97,97,"XXI"),(98,99,"XXII")
]
def chapter_to_section(ch: int|None) -> str|None:
    if ch is None: 
        return None
    for lo, hi, sec in SECTIONS:
        if lo <= int(ch) <= hi:
            return sec
    return None
