package sortindex

func WriteSortIndexMock(segkey string, cname string, data map[string]map[uint16][]uint16) error {
	return writeSortIndex(segkey, cname, data)
}
