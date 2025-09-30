#!/usr/bin/env python3

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

import fuse
from fuse import Fuse

if not hasattr(fuse, "__version__"):
    raise RuntimeError(
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
    )

fuse.fuse_python_api = (0, 2)


class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class QAFS(Fuse):
    def __init__(self, *args, **kwargs):
        Fuse.__init__(self, *args, **kwargs)
        self.storage_path = None
        self.logging = False
        self.persistent_changes = True
        self.total_size = 1024 * 1024 * 1024  # 1GB default
        self.free_size = 512 * 1024 * 1024  # 512MB default
        self.in_memory_changes = {}  # For ephemeral changes
        self.xml_files_buffer = {}  # Buffer for *_files.xml content being written

    def fsinit(self):
        if not self.storage_path:
            raise RuntimeError("Storage path not set")
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

    def _get_json_path(self, folder_name):
        return os.path.join(self.storage_path, folder_name + ".json")

    def _load_folder_data(self, folder_name):
        json_path = self._get_json_path(folder_name)
        if os.path.exists(json_path):
            with open(json_path, "r") as f:
                data = json.load(f)
                # Migrate old format to new format
                if "folders" not in data:
                    data["folders"] = {}
                return data
        return {"files": {}, "folders": {}}

    def _save_folder_data(self, folder_name, data):
        if self.persistent_changes:
            json_path = self._get_json_path(folder_name)
            with open(json_path, "w") as f:
                json.dump(data, f, indent=2)

    def _get_folders(self):
        folders = []
        for filename in os.listdir(self.storage_path):
            if filename.endswith(".json"):
                folders.append(filename[:-5])  # Remove .json extension
        return folders

    def _merge_ephemeral_changes(self, data, root_name):
        """Merge in-memory changes into the loaded data structure"""
        if not self.in_memory_changes:
            return data

        # Apply ephemeral changes to the data structure
        for key, value in self.in_memory_changes.items():
            parts = key.split("/")
            if parts[0] != root_name:
                continue

            # Navigate to parent and insert the change
            path_parts = parts[1:]
            if not path_parts:
                continue

            current = data
            for i, part in enumerate(path_parts[:-1]):
                if part not in current.get("folders", {}):
                    # Create missing intermediate folders
                    current["folders"][part] = {"files": {}, "folders": {}}
                current = current["folders"][part]

            # Insert the final node
            last_part = path_parts[-1]
            if isinstance(value, dict) and "folders" in value:
                # It's a folder
                current["folders"][last_part] = value
            else:
                # It's a file
                current["files"][last_part] = value

        return data

    def _navigate_to_parent(self, root_name, path_parts, data=None):
        """Navigate to the parent folder of the given path, return (parent_dict, last_component, root_data)"""
        if data is None:
            data = self._load_folder_data(root_name)

            # Merge ephemeral changes if not in persistent mode
            if not self.persistent_changes:
                data = self._merge_ephemeral_changes(data, root_name)

        current = data

        for i, part in enumerate(path_parts[:-1]):
            if part in current.get("folders", {}):
                current = current["folders"][part]
            else:
                return None, None, None

        return current, path_parts[-1] if path_parts else None, data

    def _navigate_to_node(self, root_name, path_parts):
        """Navigate to the exact node (file or folder) at the given path"""
        data = self._load_folder_data(root_name)

        # Merge ephemeral changes if not in persistent mode
        if not self.persistent_changes:
            data = self._merge_ephemeral_changes(data, root_name)

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
        """Get data considering both persistent and in-memory changes"""
        parts = path.strip("/").split("/")
        if len(parts) < 2:
            return None

        root_name = parts[0]
        path_parts = parts[1:]

        # Check in-memory changes first
        key = path.strip("/")
        if key in self.in_memory_changes:
            return self.in_memory_changes[key]

        # Navigate to the file
        return self._navigate_to_node(root_name, path_parts)

    def log(self, method, logline):
        with open(os.path.join(self.storage_path, "log.txt"), "a") as f:
            f.write(
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')}: {method} - {logline}\n"
            )

    def getattr(self, path):
        if self.logging:
            self.log("getattr", path)
        st = MyStat()

        if path == "/":
            # Root directory
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
            return st

        if path == "/.control":
            # Control file
            st.st_mode = stat.S_IFREG | 0o600
            st.st_nlink = 1
            st.st_size = 0
            st.st_uid = os.getuid()
            st.st_gid = os.getgid()
            st.st_atime = int(time.time())
            st.st_mtime = int(time.time())
            st.st_ctime = int(time.time())
            return st

        parts = path.strip("/").split("/")

        # Check for *_files.xml at root level (only if being written)
        if len(parts) == 1 and parts[0].endswith("_files.xml"):
            filename = parts[0]
            if filename in self.xml_files_buffer:
                st.st_mode = stat.S_IFREG | 0o644
                st.st_nlink = 1
                st.st_size = len(self.xml_files_buffer[filename])
                st.st_uid = os.getuid()
                st.st_gid = os.getgid()
                st.st_atime = int(time.time())
                st.st_mtime = int(time.time())
                st.st_ctime = int(time.time())
                return st
            else:
                return -errno.ENOENT

        root_name = parts[0]

        # Check if root folder exists
        if root_name not in self._get_folders():
            return -errno.ENOENT

        if len(parts) == 1:
            # Root-level folder (top-level JSON file)
            folder_data = self._load_folder_data(root_name)
            st.st_mode = stat.S_IFDIR | folder_data.get("mode", 0o755)
            st.st_atime = folder_data.get("atime", int(time.time()))
            st.st_mtime = folder_data.get("mtime", int(time.time()))
            st.st_ctime = folder_data.get("ctime", int(time.time()))
            st.st_uid = folder_data.get("uid", os.getuid())
            st.st_gid = folder_data.get("gid", os.getgid())
            st.st_nlink = 2
            return st

        # Navigate to nested path
        path_parts = parts[1:]
        node_data = self._navigate_to_node(root_name, path_parts)

        if node_data is None:
            return -errno.ENOENT

        # Check if it's a folder (has 'folders' and 'files' keys) or a file
        if isinstance(node_data, dict) and "folders" in node_data:
            # It's a folder
            st.st_mode = stat.S_IFDIR | node_data.get("mode", 0o755)
            st.st_atime = node_data.get("atime", int(time.time()))
            st.st_mtime = node_data.get("mtime", int(time.time()))
            st.st_ctime = node_data.get("ctime", int(time.time()))
            st.st_uid = node_data.get("uid", os.getuid())
            st.st_gid = node_data.get("gid", os.getgid())
            st.st_nlink = 2
            return st
        else:
            # It's a file
            st.st_mode = stat.S_IFREG | node_data.get("mode", 0o644)
            st.st_nlink = 1
            st.st_size = node_data.get("size", 0)
            st.st_uid = node_data.get("uid", os.getuid())
            st.st_gid = node_data.get("gid", os.getgid())
            st.st_atime = node_data.get("atime", int(time.time()))
            st.st_mtime = node_data.get("mtime", int(time.time()))
            st.st_ctime = node_data.get("ctime", int(time.time()))
            return st

    def readdir(self, path, offset):
        if self.logging:
            self.log("readdir", f"{path} @ {offset}")
        if path == "/":
            # Root directory
            yield fuse.Direntry(".")
            yield fuse.Direntry("..")
            yield fuse.Direntry(".control")
            for folder in self._get_folders():
                yield fuse.Direntry(folder)
        else:
            # Nested directory
            parts = path.strip("/").split("/")
            root_name = parts[0]

            if root_name not in self._get_folders():
                return

            if len(parts) == 1:
                # Root-level folder
                folder_data = self._load_folder_data(root_name)
                yield fuse.Direntry(".")
                yield fuse.Direntry("..")
                for filename in folder_data.get("files", {}):
                    yield fuse.Direntry(filename)
                for foldername in folder_data.get("folders", {}):
                    yield fuse.Direntry(foldername)
            else:
                # Nested folder
                path_parts = parts[1:]
                node_data = self._navigate_to_node(root_name, path_parts)

                if node_data and isinstance(node_data, dict) and "folders" in node_data:
                    yield fuse.Direntry(".")
                    yield fuse.Direntry("..")
                    for filename in node_data.get("files", {}):
                        yield fuse.Direntry(filename)
                    for foldername in node_data.get("folders", {}):
                        yield fuse.Direntry(foldername)

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

        return -errno.ENOENT

    def read(self, path, size, offset):
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
                while current + 1024 < offset:
                    rnd.randbytes(1024)  # discard 1k
                    current += 1024
                rnd.randbytes(offset - current)  # last bits

                if offset + size > file_data["size"]:
                    size = file_data["size"] - offset
                return rnd.randbytes(size)
            else:
                return b""

        return -errno.ENOENT

    def write(self, path, buf, offset):
        if self.logging:
            self.log("write", f"{path} @ {offset}")
        if path == "/.control":
            # Handle control commands
            command = buf.decode().strip()
            if command.startswith("persistent "):
                value = command.split(" ", 1)[1].lower()
                self.persistent_changes = value in ("true", "1", "yes", "on")
            elif command.startswith("total_size "):
                self.total_size = int(command.split(" ", 1)[1])
            elif command.startswith("free_size "):
                self.free_size = int(command.split(" ", 1)[1])
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
            return -errno.ENOENT

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self._get_folders():
            return -errno.ENOENT

        # Get current file data
        file_data = self._get_effective_data(path)
        if file_data and isinstance(file_data, dict) and "folders" in file_data:
            # This is a folder, not a file
            return -errno.EISDIR

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

        if self.persistent_changes:
            parent, filename, data = self._navigate_to_parent(root_name, path_parts)
            if parent is not None:
                parent["files"][filename] = file_data
                self._save_folder_data(root_name, data)
            else:
                return -errno.ENOENT
        else:
            key = path.strip("/")
            self.in_memory_changes[key] = file_data

        return len(buf)

    def create(self, path, mode, finfo):
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
            return -errno.ENOENT

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self._get_folders():
            return -errno.ENOENT

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

        if self.persistent_changes:
            parent, filename, data = self._navigate_to_parent(root_name, path_parts)
            if parent is not None:
                parent["files"][filename] = file_data
                self._save_folder_data(root_name, data)
                return 0
            return -errno.ENOENT
        else:
            key = path.strip("/")
            self.in_memory_changes[key] = file_data
            return 0

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
            self._save_folder_data(folder_name, folder_data)
            return 0

        # Create nested folder
        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self._get_folders():
            return -errno.ENOENT

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

        if self.persistent_changes:
            parent, foldername, data = self._navigate_to_parent(root_name, path_parts)
            if parent is not None:
                parent["folders"][foldername] = folder_data_new
                self._save_folder_data(root_name, data)
                return 0
            return -errno.ENOENT
        else:
            # For ephemeral mode, store in memory
            key = path.strip("/")
            self.in_memory_changes[key] = folder_data_new
            return 0

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
            return -errno.ENOENT

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self._get_folders():
            return -errno.ENOENT

        if self.persistent_changes:
            parent, filename, data = self._navigate_to_parent(root_name, path_parts)
            if parent is not None and filename in parent.get("files", {}):
                del parent["files"][filename]
                self._save_folder_data(root_name, data)
                return 0
            return -errno.ENOENT
        else:
            key = path.strip("/")
            if key in self.in_memory_changes:
                del self.in_memory_changes[key]
                return 0
            return -errno.ENOENT

    def truncate(self, path, size):
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
            return -errno.ENOENT

        root_name = parts[0]
        path_parts = parts[1:]

        if root_name not in self._get_folders():
            return -errno.ENOENT

        file_data = self._get_effective_data(path)
        if file_data and not (isinstance(file_data, dict) and "folders" in file_data):
            file_data["size"] = size
            file_data["mtime"] = int(time.time())

            if self.persistent_changes:
                parent, filename, data = self._navigate_to_parent(root_name, path_parts)
                if parent is not None:
                    parent["files"][filename] = file_data
                    self._save_folder_data(root_name, data)
                    return 0
                return -errno.ENOENT
            else:
                key = path.strip("/")
                self.in_memory_changes[key] = file_data
                return 0

        return -errno.ENOENT

    def release(self, path, flags):
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
                    if files_data:
                        self._save_folder_data(identifier, files_data)
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

    def statfs(self):
        if self.logging:
            self.log("statfs", "")
        stv = fuse.StatVfs()
        stv.f_bsize = 4096
        stv.f_frsize = 4096
        stv.f_blocks = self.total_size // 4096
        stv.f_bavail = self.free_size // 4096
        stv.f_bfree = stv.f_bavail
        stv.f_files = 1000000
        stv.f_ffree = 1000000
        return stv


def main():
    usage = (
        """
QAFS: A Linux File System in Userspace (FUSE) for Quality Assurance

"""
        + Fuse.fusage
    )

    server = QAFS(
        version="%prog " + fuse.__version__, usage=usage, dash_s_do="setsingle"
    )

    server.parser.add_option(
        mountopt="storage", metavar="PATH", default=None, help="storage directory path"
    )
    server.parser.add_option(
        mountopt="log",
        default=False,
        action="store_true",
        help="whether to log to storage/log.txt",
    )

    server.parse(errex=1)

    if server.fuse_args.mount_expected() and not server.cmdline[0].storage:
        print("Error: storage path is required", file=sys.stderr)
        sys.exit(1)

    server.storage_path = server.cmdline[0].storage
    server.logging = server.cmdline[0].log
    if server.logging:
        server.log("main", "starting")
    server.main()


if __name__ == "__main__":
    main()
