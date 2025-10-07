#!/usr/bin/env python3

# MIT License

# Copyright (c) 2025 Pablo Duboue

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import os
import sys
import stat
import errno
import json
import time
import xml.etree.ElementTree as ET
import random
from collections import defaultdict

from fusepy import FUSE, FuseOSError, Operations


class QAFS(Operations):
    def __init__(self, storage_path, logging=False):
        super(QAFS, self).__init__()
        self.storage_path = storage_path
        self.logging = logging
        self.persistent_changes = True
        self.total_size = 1024 * 1024 * 1024  # 1GB default
        self.free_size = 512 * 1024 * 1024  # 512MB default
        self.in_memory = {}  # All data is kept in RAM and saved as needed
        self.xml_files_buffer = {}  # Buffer for *_files.xml content being written

        if not self.storage_path:
            raise RuntimeError("Storage path not set")
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

        self.load()

    def init(self, path):
        """Called on filesystem initialization"""
        pass

    def load(self):
        for filename in os.listdir(self.storage_path):
            if filename.endswith(".json"):
                top_folder = filename[:-5]  # Remove .json extension
                with open(os.path.join(self.storage_path, filename), "r") as f:
                    self.in_memory[top_folder] = json.load(f)
                    self.log("load", top_folder)

    def _get_json_path(self, folder_name):
        """Get the JSON file path for a root folder"""
        return os.path.join(self.storage_path, f"{folder_name}.json")

    def _save_folder_data(self, folder_name):
        if self.persistent_changes:
            json_path = self._get_json_path(folder_name)
            with open(json_path, "w") as f:
                json.dump(self.in_memory[folder_name], f, indent=2)
            
    def _navigate_to_parent(self, root_name, path_parts):
        """Navigate to the parent folder of the given path, return (parent_dict, last_component, root_data)"""
        data = self.in_memory[root_name]
        current = data

        for i, part in enumerate(path_parts[:-1]):
            if part in current.get("folders", {}):
                current = current["folders"][part]
            else:
                return None, None, None

        return current, path_parts[-1] if path_parts else None, data

    def _navigate_to_node(self, root_name, path_parts=None):
        """Navigate to the exact node (file or folder) at the given path"""
        data = self.in_memory[root_name]

        if not path_parts:
            return data

        current = data
        for part in path_parts:
            if part in current.get("folders", {}):
                current = current["folders"][part]
            elif part in current.get("files", {}):
                return current["files"][part]
            else:
                return None

        return current

    def _parse_files_xml(self, xml_content):
        try:
            root = ET.fromstring(xml_content)
            files_data = {
                "files": {},
                "folders": {},
                "mode": 0o755,
                "uid": os.getuid(),
                "gid": os.getgid(),
                "atime": int(time.time()),
                "mtime": int(time.time()),
                "ctime": int(time.time()),
            }

            for file_elem in root.findall(".//file"):
                name = file_elem.get("name", "")
                if not name:
                    continue

                # Get size from child element
                size_elem = file_elem.find("size")
                size = (
                    int(size_elem.text)
                    if size_elem is not None and size_elem.text
                    else 0
                )

                # Get mtime from child element
                mtime_elem = file_elem.find("mtime")
                mtime = (
                    int(mtime_elem.text)
                    if mtime_elem is not None and mtime_elem.text
                    else int(time.time())
                )

                # Handle files with path separators (create nested folders)
                path_parts = name.split("/")
                if len(path_parts) > 1:
                    # File is in a nested folder
                    current = files_data
                    for folder_name in path_parts[:-1]:
                        if folder_name not in current["folders"]:
                            current["folders"][folder_name] = {
                                "files": {},
                                "folders": {},
                                "mode": 0o755,
                                "uid": os.getuid(),
                                "gid": os.getgid(),
                                "atime": int(time.time()),
                                "mtime": int(time.time()),
                                "ctime": int(time.time()),
                            }
                        current = current["folders"][folder_name]

                    # Add file to the deepest folder
                    filename = path_parts[-1]
                    current["files"][filename] = {
                        "size": size,
                        "mode": 0o644,
                        "uid": os.getuid(),
                        "gid": os.getgid(),
                        "atime": int(time.time()),
                        "mtime": mtime,
                        "ctime": int(time.time()),
                        "content_seed": sum(map(ord, name)),
                    }
                else:
                    # File is in root of this folder
                    files_data["files"][name] = {
                        "size": size,
                        "mode": 0o644,
                        "uid": os.getuid(),
                        "gid": os.getgid(),
                        "atime": int(time.time()),
                        "mtime": mtime,
                        "ctime": int(time.time()),
                        "content_seed": sum(map(ord, name)),
                    }

            return files_data
        except ET.ParseError as e:
            return None

    def _get_effective_data(self, path):
        parts = path.strip("/").split("/")
        if len(parts) < 2:
            return None

        root_name = parts[0]
        path_parts = parts[1:]

        return self._navigate_to_node(root_name, path_parts)

    def log(self, method, logline):
        with open(os.path.join(self.storage_path, "log.txt"), "a") as f:
            f.write(
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')}: {method} - {logline}\n"
            )

    def getattr(self, path, fh=None):
        if self.logging:
            self.log("getattr", path)

        if path == "/":
            # Root directory
            return dict(
                st_mode=(stat.S_IFDIR | 0o755),
                st_nlink=2,
                st_atime=int(time.time()),
                st_mtime=int(time.time()),
                st_ctime=int(time.time())
            )

        if path == "/.control":
            # Control file
            return dict(
                st_mode=(stat.S_IFREG | 0o600),
                st_nlink=1,
                st_size=0,
                st_uid=os.getuid(),
                st_gid=os.getgid(),
                st_atime=int(time.time()),
                st_mtime=int(time.time()),
                st_ctime=int(time.time())
            )

        parts = path.strip("/").split("/")

        # Check for *_files.xml at root level (only if being written)
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            if filename in self.xml_files_buffer:
                return dict(
                    st_mode=(stat.S_IFREG | 0o644),
                    st_nlink=1,
                    st_size=len(self.xml_files_buffer[filename]),
                    st_uid=os.getuid(),
                    st_gid=os.getgid(),
                    st_atime=int(time.time()),
                    st_mtime=int(time.time()),
                    st_ctime=int(time.time())
                )
            else:
                raise FuseOSError(errno.ENOENT)

        root_name = parts[0]

        # Check if root folder exists
        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        if len(parts) == 1:
            # Root-level folder (top-level JSON file)
            folder_data = self.in_memory[root_name]
            return dict(
                st_mode=(stat.S_IFDIR | folder_data.get("mode", 0o755)),
                st_nlink=2,
                st_atime=folder_data.get("atime", int(time.time())),
                st_mtime=folder_data.get("mtime", int(time.time())),
                st_ctime=folder_data.get("ctime", int(time.time())),
                st_uid=folder_data.get("uid", os.getuid()),
                st_gid=folder_data.get("gid", os.getgid())
            )

        # Navigate to nested path
        path_parts = parts[1:]
        node_data = self._navigate_to_node(root_name, path_parts)

        if node_data is None:
            raise FuseOSError(errno.ENOENT)

        # Check if it's a folder (has 'folders' and 'files' keys) or a file
        if isinstance(node_data, dict) and "folders" in node_data:
            # It's a folder
            return dict(
                st_mode=(stat.S_IFDIR | node_data.get("mode", 0o755)),
                st_nlink=2,
                st_atime=node_data.get("atime", int(time.time())),
                st_mtime=node_data.get("mtime", int(time.time())),
                st_ctime=node_data.get("ctime", int(time.time())),
                st_uid=node_data.get("uid", os.getuid()),
                st_gid=node_data.get("gid", os.getgid())
            )
        else:
            # It's a file
            return dict(
                st_mode=(stat.S_IFREG | node_data.get("mode", 0o644)),
                st_nlink=1,
                st_size=node_data.get("size", 0),
                st_uid=node_data.get("uid", os.getuid()),
                st_gid=node_data.get("gid", os.getgid()),
                st_atime=node_data.get("atime", int(time.time())),
                st_mtime=node_data.get("mtime", int(time.time())),
                st_ctime=node_data.get("ctime", int(time.time())),
                st_blocks=node_data.get("size", 0) // 4096 + (0 if node_data.get("size", 0) % 4096 == 0 else 1),
                st_blksize=4096
            )

    def readdir(self, path, fh):
        if self.logging:
            self.log("readdir", f"{path}")

        dirents = ['.', '..']

        if path == "/":
            # Root directory
            dirents.append(".control")
            dirents.extend(self.in_memory.keys())
        else:
            # Nested directory
            parts = path.strip("/").split("/")
            root_name = parts[0]

            if root_name not in self.in_memory:
                raise FuseOSError(errno.ENOENT)

            if len(parts) == 1:
                # Root-level folder
                folder_data = self.in_memory[root_name]
                dirents.extend(folder_data.get("files", {}).keys())
                dirents.extend(folder_data.get("folders", {}).keys())
            else:
                # Nested folder
                path_parts = parts[1:]
                node_data = self._navigate_to_node(root_name, path_parts)

                if node_data and isinstance(node_data, dict) and "folders" in node_data:
                    dirents.extend(node_data.get("files", {}).keys())
                    dirents.extend(node_data.get("folders", {}).keys())
                else:
                    raise FuseOSError(errno.ENOTDIR)

        return dirents

    def open(self, path, flags):
        if self.logging:
            self.log("open", f"{path} flags: {flags}")
        if path == "/.control":
            return 0

        parts = path.strip("/").split("/")

        # Allow opening *_files.xml at root level for writing
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            return 0

        if len(parts) >= 2:
            file_data = self._get_effective_data(path)
            if (
                file_data
                and not isinstance(file_data, dict)
                or (isinstance(file_data, dict) and "folders" not in file_data)
            ):
                return 0

        raise FuseOSError(errno.ENOENT)

    def read(self, path, size, offset, fh):
        if self.logging:
            self.log("read", f"{path} {size} @ {offset}")
        if path == "/.control":
            return b""

        file_data = self._get_effective_data(path)
        if file_data:
            content_seed = file_data.get("content_seed", sum(map(ord, path)))

            if offset < file_data["size"]:
                current = 0
                rnd = random.Random(content_seed)
                while current + 4096 < offset:
                    rnd.randbytes(4096)  # discard 4k
                    current += 4096
                rnd.randbytes(offset - current)  # last bits

                if offset + size > file_data["size"]:
                    size = file_data["size"] - offset
                return rnd.randbytes(size)
            else:
                return b""

        raise FuseOSError(errno.ENOENT)

    def write(self, path, buf, offset, fh):
        if self.logging:
            self.log("write", f"{path} @ {offset}")
        if path == "/.control":
            # Handle control commands
            command = buf.decode().strip()
            if command.startswith("persistent "):
                value = command.split(" ", 1)[1].lower()
                self.persistent_changes = value in ("true", "1", "yes", "on")
                self.log("write", f"now persistent = {self.persistent_changes}")
            elif command.startswith("total_size "):
                self.total_size = int(command.split(" ", 1)[1])
                self.log("write", f"now total_size = {self.total_size}")
            elif command.startswith("free_size "):
                self.free_size = int(command.split(" ", 1)[1])
                self.log("write", f"now free_size = {self.free_size}")
            return len(buf)

        parts = path.strip("/").split("/")

        # Special handling for *_files.xml at root: buffer content
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            if filename not in self.xml_files_buffer:
                self.xml_files_buffer[filename] = bytearray()

            # Expand buffer if needed
            if offset + len(buf) > len(self.xml_files_buffer[filename]):
                self.xml_files_buffer[filename].extend(
                    b"\0" * (offset + len(buf) - len(self.xml_files_buffer[filename]))
                )

            # Write data at offset
            self.xml_files_buffer[filename][offset : offset + len(buf)] = buf
            return len(buf)

        if len(parts) < 2:
            raise FuseOSError(errno.ENOENT)

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        # Get current file data
        file_data = self._get_effective_data(path)
        if file_data and isinstance(file_data, dict) and "folders" in file_data:
            # This is a folder, not a file
            raise FuseOSError(errno.EISDIR)

        if not file_data:
            file_data = {
                "size": 0,
                "mode": 0o644,
                "uid": os.getuid(),
                "gid": os.getgid(),
                "atime": int(time.time()),
                "mtime": int(time.time()),
                "ctime": int(time.time()),
                "content_seed": sum(map(ord, path)),
            }

        # Extend content if necessary
        if offset + len(buf) > file_data["size"]:
            file_data["size"] = offset + len(buf)
        file_data["mtime"] = int(time.time())

        parent, filename, data = self._navigate_to_parent(root_name, path_parts)
        if parent is not None:
            parent["files"][filename] = file_data
            self._save_folder_data(root_name)
        else:
            raise FuseOSError(errno.ENOENT)

        return len(buf)

    def utimens(self, path, times=None):
        if self.logging:
            self.log("utimens", f"{path} - {times}")
        if path == "/.control":
            raise FuseOSError(errno.ENOENT)

        parts = path.strip("/").split("/")

        if len(parts) >= 1:
            # check it is a known folder
            root_name = parts[0]
            if root_name not in self.in_memory:
                raise FuseOSError(errno.ENOENT)
            if len(parts) == 1:
                folder_data = self.in_memory[root_name]
                folder_data["atime"] = int(time.time()) if times is None else int(times[0])
                folder_data["mtime"] = int(time.time()) if times is None else int(times[1])
                self._save_folder_data(root_name)
                return 0

        path_parts = parts[1:]

        # Get current file data
        file_data = self._get_effective_data(path)
        if not file_data:
            raise FuseOSError(errno.ENOENT)
        file_data["atime"] = int(time.time()) if times is None else int(times[0])
        file_data["mtime"] = int(time.time()) if times is None else int(times[1])

        parent, filename, data = self._navigate_to_parent(root_name, path_parts)
        if parent is not None:
            if isinstance(file_data, dict) and "folders" in file_data:
                parent["folders"][filename] = file_data
            else:
                parent["files"][filename] = file_data
            self._save_folder_data(root_name)
        else:
            raise FuseOSError(errno.ENOENT)

        return 0

    def create(self, path, mode, fi=None):
        if self.logging:
            self.log("create", f"{path} mode {mode}")

        parts = path.strip("/").split("/")

        # Special handling for Internet Archive *_files.xml at root level
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            # Initialize buffer
            self.xml_files_buffer[filename] = bytearray()
            return 0

        if len(parts) < 2:
            raise FuseOSError(errno.ENOENT)

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        file_data = {
            "size": 0,
            "mode": mode,
            "uid": os.getuid(),
            "gid": os.getgid(),
            "atime": int(time.time()),
            "mtime": int(time.time()),
            "ctime": int(time.time()),
            "content_seed": sum(map(ord, path)),
        }

        parent, filename, data = self._navigate_to_parent(root_name, path_parts)
        if parent is not None:
            parent["files"][filename] = file_data
            self._save_folder_data(root_name)
            return 0
        raise FuseOSError(errno.ENOENT)

    def mkdir(self, path, mode):
        if self.logging:
            self.log("mkdir", f"{path} mode {mode}")
        parts = path.strip("/").split("/")

        if len(parts) == 1:
            # Create root-level folder (new JSON file)
            folder_name = parts[0]
            folder_data = {
                "files": {},
                "folders": {},
                "mode": mode,
                "uid": os.getuid(),
                "gid": os.getgid(),
                "atime": int(time.time()),
                "mtime": int(time.time()),
                "ctime": int(time.time()),
            }
            self.in_memory[folder_name] = folder_data
            self._save_folder_data(folder_name)
            return 0

        # Create nested folder
        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        folder_data_new = {
            "files": {},
            "folders": {},
            "mode": mode,
            "uid": os.getuid(),
            "gid": os.getgid(),
            "atime": int(time.time()),
            "mtime": int(time.time()),
            "ctime": int(time.time()),
        }

        parent, foldername, data = self._navigate_to_parent(root_name, path_parts)
        if parent is not None:
            parent["folders"][foldername] = folder_data_new
            self._save_folder_data(root_name)
            return 0
        raise FuseOSError(errno.ENOENT)

    def unlink(self, path):
        if self.logging:
            self.log("unlink", path)

        parts = path.strip("/").split("/")

        # Handle *_files.xml at root: just remove from buffer if present
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            if filename in self.xml_files_buffer:
                del self.xml_files_buffer[filename]
            return 0
        if len(parts) < 2:
            raise FuseOSError(errno.ENOENT)

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        parent, filename, data = self._navigate_to_parent(root_name, path_parts)
        if parent is not None and filename in parent.get("files", {}):
            del parent["files"][filename]
            self._save_folder_data(root_name)
            return 0
        raise FuseOSError(errno.ENOENT)

    def truncate(self, path, size, fh=None):
        if self.logging:
            self.log("truncate", f"{path} to {size}")

        if path == "/.control":
            return 0

        parts = path.strip("/").split("/")

        # Handle *_files.xml at root: initialize buffer
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            if filename not in self.xml_files_buffer:
                self.xml_files_buffer[filename] = bytearray()
            # Resize buffer
            if size < len(self.xml_files_buffer[filename]):
                self.xml_files_buffer[filename] = self.xml_files_buffer[filename][:size]
            elif size > len(self.xml_files_buffer[filename]):
                self.xml_files_buffer[filename].extend(
                    b"\0" * (size - len(self.xml_files_buffer[filename]))
                )
            return 0

        if len(parts) < 2:
            raise FuseOSError(errno.ENOENT)

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        file_data = self._get_effective_data(path)
        if file_data and not (isinstance(file_data, dict) and "folders" in file_data):
            file_data["size"] = size
            file_data["mtime"] = int(time.time())

            parent, filename, data = self._navigate_to_parent(root_name, path_parts)
            if parent is not None:
                parent["files"][filename] = file_data
                self._save_folder_data(root_name)
                return 0
            raise FuseOSError(errno.ENOENT)

        raise FuseOSError(errno.ENOENT)

    def release(self, path, fh):
        """Called when a file is closed"""
        if self.logging:
            self.log("release", path)

        parts = path.strip("/").split("/")

        # Special handling for *_files.xml at root level
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            # Extract identifier from filename: identifier_files.xml -> identifier
            identifier = filename[:-10]  # Remove '_files.xml'

            if filename in self.xml_files_buffer:
                try:
                    xml_content = bytes(self.xml_files_buffer[filename]).decode("utf-8")
                    files_data = self._parse_files_xml(xml_content)
                    self.in_memory[identifier] = files_data
                    if files_data:
                        self._save_folder_data(identifier)
                        if self.logging:
                            self.log(
                                "release",
                                f"Created folder {identifier} from {filename}",
                            )
                except Exception as e:
                    if self.logging:
                        self.log("release", f"Error processing {filename}: {e}")

                # Clean up buffer
                del self.xml_files_buffer[filename]

        return 0

    def rmdir(self, path):
        """Remove a directory"""
        if self.logging:
            self.log("rmdir", path)

        parts = path.strip("/").split("/")

        if len(parts) == 1:
            # Remove root-level folder (delete JSON file and in-memory data)
            folder_name = parts[0]
            if folder_name not in self.in_memory:
                raise FuseOSError(errno.ENOENT)

            folder_data = self.in_memory[folder_name]
            # Check if folder is empty
            if folder_data.get("files", {}) or folder_data.get("folders", {}):
                raise FuseOSError(errno.ENOTEMPTY)

            # Remove from memory
            del self.in_memory[folder_name]

            # Remove JSON file if in persistent mode
            if self.persistent_changes:
                json_path = os.path.join(self.storage_path, f"{folder_name}.json")
                if os.path.exists(json_path):
                    os.remove(json_path)
            return 0

        # Remove nested folder
        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        # Get the folder to be removed
        folder_data = self._navigate_to_node(root_name, path_parts)
        if not folder_data or not isinstance(folder_data, dict) or "folders" not in folder_data:
            raise FuseOSError(errno.ENOTDIR)

        # Check if folder is empty
        if folder_data.get("files", {}) or folder_data.get("folders", {}):
            raise FuseOSError(errno.ENOTEMPTY)

        # Remove from parent
        parent, foldername, data = self._navigate_to_parent(root_name, path_parts)
        if parent is not None and foldername in parent.get("folders", {}):
            del parent["folders"][foldername]
            self._save_folder_data(root_name)
            return 0

        raise FuseOSError(errno.ENOENT)

    def rename(self, old_path, new_path):
        """Rename a file or directory"""
        if self.logging:
            self.log("rename", f"{old_path} -> {new_path}")

        old_parts = old_path.strip("/").split("/")
        new_parts = new_path.strip("/").split("/")

        # Handle root-level folder rename
        if len(old_parts) == 1 and len(new_parts) == 1:
            old_name = old_parts[0]
            new_name = new_parts[0]

            if old_name not in self.in_memory:
                raise FuseOSError(errno.ENOENT)

            if new_name in self.in_memory:
                raise FuseOSError(errno.EEXIST)

            # Rename in memory
            self.in_memory[new_name] = self.in_memory[old_name]
            del self.in_memory[old_name]

            # Rename JSON file if in persistent mode
            if self.persistent_changes:
                old_json = os.path.join(self.storage_path, f"{old_name}.json")
                new_json = os.path.join(self.storage_path, f"{new_name}.json")
                if os.path.exists(old_json):
                    os.rename(old_json, new_json)
            return 0

        # Handle nested file/folder rename
        if len(old_parts) < 2 or len(new_parts) < 2:
            raise FuseOSError(errno.ENOENT)

        old_root = old_parts[0]
        new_root = new_parts[0]

        # Must be within the same root folder
        if old_root != new_root:
            raise FuseOSError(errno.EXDEV)

        if old_root not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        old_path_parts = old_parts[1:]
        new_path_parts = new_parts[1:]

        # Get the node to be renamed
        node_data = self._navigate_to_node(old_root, old_path_parts)
        if not node_data:
            raise FuseOSError(errno.ENOENT)

        # Determine if it's a file or folder
        is_folder = isinstance(node_data, dict) and "folders" in node_data

        # Get old and new parent
        old_parent, old_name, _ = self._navigate_to_parent(old_root, old_path_parts)
        new_parent, new_name, _ = self._navigate_to_parent(new_root, new_path_parts)

        if old_parent is None or new_parent is None:
            raise FuseOSError(errno.ENOENT)

        # Check if destination already exists
        if is_folder:
            if new_name in new_parent.get("folders", {}):
                raise FuseOSError(errno.EEXIST)
        else:
            if new_name in new_parent.get("files", {}):
                raise FuseOSError(errno.EEXIST)

        # Perform the rename
        if is_folder:
            new_parent["folders"][new_name] = old_parent["folders"][old_name]
            del old_parent["folders"][old_name]
        else:
            new_parent["files"][new_name] = old_parent["files"][old_name]
            del old_parent["files"][old_name]

        self._save_folder_data(old_root)
        return 0

    def chmod(self, path, mode):
        """Change file/directory permissions"""
        if self.logging:
            self.log("chmod", f"{path} mode {oct(mode)}")

        if path == "/" or path == "/.control":
            raise FuseOSError(errno.EPERM)

        parts = path.strip("/").split("/")

        if len(parts) == 1:
            # Root-level folder
            folder_name = parts[0]
            if folder_name not in self.in_memory:
                raise FuseOSError(errno.ENOENT)

            self.in_memory[folder_name]["mode"] = stat.S_IMODE(mode)
            self.in_memory[folder_name]["ctime"] = int(time.time())
            self._save_folder_data(folder_name)
            return 0

        # Nested file or folder
        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        node_data = self._navigate_to_node(root_name, path_parts)
        if not node_data:
            raise FuseOSError(errno.ENOENT)

        node_data["mode"] = stat.S_IMODE(mode)
        node_data["ctime"] = int(time.time())

        # Save the changes
        parent, name, _ = self._navigate_to_parent(root_name, path_parts)
        if parent is not None:
            is_folder = isinstance(node_data, dict) and "folders" in node_data
            if is_folder:
                parent["folders"][name] = node_data
            else:
                parent["files"][name] = node_data
            self._save_folder_data(root_name)
            return 0

        raise FuseOSError(errno.ENOENT)

    def chown(self, path, uid, gid):
        """Change file/directory owner"""
        if self.logging:
            self.log("chown", f"{path} uid {uid} gid {gid}")

        if path == "/" or path == "/.control":
            raise FuseOSError(errno.EPERM)

        parts = path.strip("/").split("/")

        if len(parts) == 1:
            # Root-level folder
            folder_name = parts[0]
            if folder_name not in self.in_memory:
                raise FuseOSError(errno.ENOENT)

            if uid != -1:
                self.in_memory[folder_name]["uid"] = uid
            if gid != -1:
                self.in_memory[folder_name]["gid"] = gid
            self.in_memory[folder_name]["ctime"] = int(time.time())
            self._save_folder_data(folder_name)
            return 0

        # Nested file or folder
        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self.in_memory:
            raise FuseOSError(errno.ENOENT)

        node_data = self._navigate_to_node(root_name, path_parts)
        if not node_data:
            raise FuseOSError(errno.ENOENT)

        if uid != -1:
            node_data["uid"] = uid
        if gid != -1:
            node_data["gid"] = gid
        node_data["ctime"] = int(time.time())

        # Save the changes
        parent, name, _ = self._navigate_to_parent(root_name, path_parts)
        if parent is not None:
            is_folder = isinstance(node_data, dict) and "folders" in node_data
            if is_folder:
                parent["folders"][name] = node_data
            else:
                parent["files"][name] = node_data
            self._save_folder_data(root_name)
            return 0

        raise FuseOSError(errno.ENOENT)

    def statfs(self, path):
        if self.logging:
            self.log("statfs", path)
        return dict(
            f_bsize=4096,
            f_frsize=4096,
            f_blocks=self.total_size // 4096,
            f_bavail=self.free_size // 4096,
            f_bfree=self.free_size // 4096,
            f_files=1000000,
            f_ffree=1000000
        )

# not implemented:

#   readlink
#   mknod
#   symlink
#   link
#   fsync
#   fsyncdir
#   flush
#   opendir
#   releasedir
#   fgetattr
#      In FUSE 2, if fgetattr is not implemented in your FUSE filesystem, the system will typically fall back to calling your getattr function, using the path associated with the file descriptor if available.
#   getxattr
#   listxattr
#   setxattr
#   removexattr
#   utime
#      When implementing a FUSE filesystem, if the utimens operation is handled, it is generally not necessary to also explicitly handle utime.
#      libfuse, the library used to implement FUSE filesystems, typically maps older system calls like utime to the more modern utimens operation internally. If a FUSE filesystem provides an implementation for utimens, libfuse will use that when a utime call is made by an application.
#   access
#      the access operation is a callback function that the kernel invokes to check if a process has the necessary permissions to access a specific file or directory. This function typically mirrors the behavior of the standard access(2) system call.
#      The access callback receives the path to the file or directory and a mode argument (e.g., R_OK for read, W_OK for write, X_OK for execute, or F_OK to check for existence). The FUSE implementation then determines if the calling process, based on its UID/GID and the file's permissions, is allowed to perform the requested access.
#   lock
#   fsdestroy
#   ioctl
#      Linux, the ioctl() system call provides a way for user-space applications to interact with device drivers and perform device-specific operations that are not covered by standard read/write operations.
#   poll


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="QAFS: A Linux File System in Userspace (FUSE) for Quality Assurance"
    )
    parser.add_argument("mountpoint", help="Mount point for the filesystem")
    parser.add_argument("-s", "--storage", required=True, help="Storage directory path")
    parser.add_argument("-l", "--log", action="store_true", help="Enable logging to storage/log.txt")
    parser.add_argument("-f", "--foreground", action="store_true", help="Run in foreground")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()

    if args.log:
        print(f"Starting QAFS with storage at {args.storage}")

    # Create the QAFS instance
    qafs = QAFS(storage_path=args.storage, logging=args.log)

    # Mount the filesystem
    FUSE(qafs, args.mountpoint, foreground=args.foreground, debug=args.debug, nothreads=True)


if __name__ == "__main__":
    main()
