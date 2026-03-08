// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package sortindex

import sutils "github.com/siglens/siglens/pkg/segment/utils"

func WriteSortIndexMock(segkey string, cname string, sortMode SortMode,
	data map[sutils.CValueEnclosure]map[uint16][]uint16) error {

	return writeSortIndex(segkey, cname, sortMode, data)
}
