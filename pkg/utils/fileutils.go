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

package utils

import (
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func SymlinkRelativePaths(oldPath string, newPath string) error {
	absOldPath, err := filepath.Abs(oldPath)
	if err != nil {
		log.Errorf("symlinkRelativePaths: failed to get absolute path for oldPath %v; err=%v",
			oldPath, err)
		return err
	}

	absNewPath, err := filepath.Abs(newPath)
	if err != nil {
		log.Errorf("symlinkRelativePaths: failed to get absolute path for newPath %v; err=%v",
			newPath, err)
		return err
	}

	err = os.Symlink(absOldPath, absNewPath)
	if err != nil {
		log.Errorf("symlinkRelativePaths: failed to symlink %v to %v; err=%v", absOldPath, absNewPath, err)
		return err
	}

	return nil
}
