// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

// Options for readDir function call
type readDirOpts struct {
	// The maximum number of entries to return
	count int // 要返回的最大条目数。
	// Follow directory symlink
	followDirSymlink bool // 指示是否应该跟随目录符号链接。
}

// 重构了 os.ReadDir方法，添加自定

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirWithOpts(dirPath, readDirOpts{count: -1})
}

// 它只返回最多 count 个条目的名称
// Return up to count entries at the directory dirPath.
func readDirN(dirPath string, count int) (entries []string, err error) {
	return readDirWithOpts(dirPath, readDirOpts{count: count})
}
