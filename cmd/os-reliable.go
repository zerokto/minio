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

import (
	"fmt"
	"os"
	"path"
)

// Wrapper functions to os.RemoveAll, which calls reliableRemoveAll
// this is to ensure that if there is a racy parent directory
// create in between we can simply retry the operation.
func removeAll(dirPath string) (err error) {
	if dirPath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		return err
	}

	if err = reliableRemoveAll(dirPath); err != nil {
		switch {
		case isSysErrNotDir(err):
			// File path cannot be verified since one of
			// the parents is a file.
			return errFileAccessDenied
		case isSysErrPathNotFound(err):
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically
			// here.
			return errFileAccessDenied
		}
	}
	return err
}

// Reliably retries os.RemoveAll if for some reason os.RemoveAll returns
// syscall.ENOTEMPTY (children has files).
func reliableRemoveAll(dirPath string) (err error) {
	i := 0
	for {
		// Removes all the directories and files.
		if err = RemoveAll(dirPath); err != nil {
			// Retry only for the first retryable error.
			if isSysErrNotEmpty(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}

// Wrapper functions to os.MkdirAll, which calls reliableMkdirAll
// this is to ensure that if there is a racy parent directory
// delete in between we can simply retry the operation.
func mkdirAll(dirPath string, mode os.FileMode) (err error) {
	if dirPath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		return err
	}

	if err = reliableMkdirAll(dirPath, mode); err != nil {
		// File path cannot be verified since one of the parents is a file.
		if isSysErrNotDir(err) {
			return errFileAccessDenied
		} else if isSysErrPathNotFound(err) {
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically here.
			return errFileAccessDenied
		}
		return osErrToFileErr(err)
	}

	return nil
}

// Reliably retries os.MkdirAll if for some reason os.MkdirAll returns
// syscall.ENOENT (parent does not exist).
// reliableMkdirAll 是一个可靠的重试函数，用于在某些情况下，
// 如果 os.MkdirAll 返回 syscall.ENOENT（父目录不存在）错误，
// 则可靠地重试 os.MkdirAll 操作。
func reliableMkdirAll(dirPath string, mode os.FileMode) (err error) {
	i := 0
	for {
		// Creates all the parent directories, with mode 0777 mkdir honors system umask.
		// 创建所有父目录，使用给定的模式 0777。mkdir 根据系统的 umask 进行权限处理。
		if err = osMkdirAll(dirPath, mode); err != nil {
			// Retry only for the first retryable error.
			if osIsNotExist(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}

// Wrapper function to os.Rename, which calls reliableMkdirAll
// and reliableRenameAll. This is to ensure that if there is a
// racy parent directory delete in between we can simply retry
// the operation.
// 包装函数，用于调用 os.Rename 进行文件重命名操作
func renameAll(srcFilePath, dstFilePath string) (err error) {
	if srcFilePath == "" || dstFilePath == "" {
		return errInvalidArgument
	}
	// 检查源文件路径长度是否超过限制
	if err = checkPathLength(srcFilePath); err != nil {
		return err
	}
	if err = checkPathLength(dstFilePath); err != nil {
		return err
	}
	// 调用 reliableRename 函数进行重命名操作
	if err = reliableRename(srcFilePath, dstFilePath); err != nil {
		switch {
		case isSysErrNotDir(err) && !osIsNotExist(err):
			// Windows can have both isSysErrNotDir(err) and osIsNotExist(err) returning
			// true if the source file path contains an non-existent directory. In that case,
			// we want to return errFileNotFound instead, which will honored in subsequent
			// switch cases
			// 对于 Windows 系统，当源文件路径包含不存在的目录时，
			// 可能同时返回 isSysErrNotDir(err) 和 osIsNotExist(err)
			// 为真。在这种情况下，我们希望返回 errFileNotFound 错误，
			// 这将在后续的 switch case 中得到处理
			return errFileAccessDenied
		case isSysErrPathNotFound(err):
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically here.
			// 这是一个特殊情况，仅应针对 Windows 处理，
			// 因为 Windows API 不会返回 "not a directory" 的错误消息。
			// 在这里特别处理此情况。
			return errFileAccessDenied
		case isSysErrCrossDevice(err):
			return fmt.Errorf("%w (%s)->(%s)", errCrossDeviceLink, srcFilePath, dstFilePath)
		case osIsNotExist(err):
			return errFileNotFound
		case osIsExist(err):
			// This is returned only when destination is a directory and we
			// are attempting a rename from file to directory.
			// 仅当目标是一个目录，并且我们尝试将文件重命名为目录时，才会返回此值。
			return errIsNotRegular
		default:
			return err
		}
	}
	return nil
}

// Reliably retries os.RenameAll if for some reason os.RenameAll returns
// syscall.ENOENT (parent does not exist).
// 这段代码定义了一个可靠的重试函数 reliableRename，用于在某些情况下，
// 如果 os.Rename 返回 syscall.ENOENT（父目录不存在）错误，
// 则可靠地重试 os.Rename 操作。
func reliableRename(srcFilePath, dstFilePath string) (err error) {
	if err = reliableMkdirAll(path.Dir(dstFilePath), 0o777); err != nil {
		return err
	}

	i := 0
	for {
		// After a successful parent directory create attempt a renameAll.
		// 在成功创建父目录后进行重命名操作
		if err = Rename(srcFilePath, dstFilePath); err != nil {
			// Retry only for the first retryable error.
			// 只对第一个可重试的错误进行重试
			if osIsNotExist(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}
