from nose.tools import assert_equal, assert_true, assert_false, raises

import subread_stats as ss

def test_ok_parsing():
    h = ss.header.parse('>smrt1/1234/0_123452')

    assert_equal(h.cell, 'smrt1')
    assert_equal(h.hole, 1234)
    assert_equal(h.range.start, 0)
    assert_equal(h.range.end, 123452)

@raises(ValueError)
def test_invalid_header_parsing():
    ss.header.parse('>smrt1_hole1234/0-12345')

@raises(ValueError)
def test_invalid_header_parsing2():
    ss.header.parse('>smrt1_hole1234/0_12345')

@raises(ValueError)
def test_invalid_header_parsing3():
    ss.header.parse('>smrt1_hole//1234/0_12345')

def test_overlapping():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
    h2 = ss.header('smrt1', 1, ss.seq_range(50, 150))

    assert_equal(h1.distance(h2), 0)
    assert_true(h1.overlaps(h2))

def test_nearly_overlapping():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
    h2 = ss.header('smrt1', 1, ss.seq_range(101, 150))

    assert_equal(h1.distance(h2), 1)
    assert_false(h1.overlaps(h2))

def test_different_holes_not_overlapping():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
    h2 = ss.header('smrt1', 2, ss.seq_range(50, 150))

    assert_false(h1.overlaps(h2))

def test_different_cells_not_overlapping():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
    h2 = ss.header('smrt2', 1, ss.seq_range(50, 150))

    assert_false(h1.overlaps(h2))

@raises(TypeError)
def test_different_holes_undefined_distance():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
    h2 = ss.header('smrt1', 2, ss.seq_range(50, 150))

    h1.distance(h2)

@raises(TypeError)
def test_different_cells_undefined_distance():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
    h2 = ss.header('smrt2', 1, ss.seq_range(50, 150))

    assert_false(h1.distance(h2))

def test_distance():
    h1 = ss.header('smrt1', 1, ss.seq_range(0, 100))
