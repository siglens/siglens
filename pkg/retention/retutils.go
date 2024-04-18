// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package retention

import (
	"io"
	"os"
	"path"

	"github.com/siglens/siglens/pkg/config"
)

func IsDirEmpty(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()

	// read in ONLY one file
	_, err = f.Readdir(1)

	// and if the file is EOF... well, the dir is empty.
	return err == io.EOF
}

func RecursivelyDeleteParentDirectories(filePath string) {
	temp := path.Dir(filePath)
	for {
		if temp == config.GetDataPath() {
			break
		}
		if IsDirEmpty(temp) {
			os.RemoveAll(temp)
		} else {
			break
		}
		temp = path.Dir(temp)
	}
}
