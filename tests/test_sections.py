from common.sections import chapter_to_section
def test_map():
    assert chapter_to_section(64) == "XII"
    assert chapter_to_section(1) == "I"
    assert chapter_to_section(99) == "XXII"
