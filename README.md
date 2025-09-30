# QAFS: A Linux File System in Userspace (FUSE) for Quality Assurance

This file system provides fake data for testing purposes. It is
intended to use for testing synchronization of files across systems.

The file system itself is stored as one JSON file per root-level folder in the
storage folder provided at mount time. Each JSON file supports arbitrary nesting
of folders and files.

The root of the file system does not host files, only folders. It can
receive Internet Archive item description `_files.xml` and it'll
create a folder for that item.

Each file has all the Unix attributes (size, owner, creation, access,
modification date, etc) provided from the JSON files which can be
easily modified to create test cases.

The file system can be interacted with, with the backed JSON files
responding to the changes (but changes to the JSON files _while_ the
file system is mounted is not supported).

A `.control` hidden file in the root folder allows to pipe commands to
it that allow changing:

* Whether the underlining JSON files are modified (otherwise the
  changes are ephemeral)

* File system total and free sizes to be reported.

## Installation

QAFS requires the python-fuse library to be installed:

```bash
pip install fuse-python
```

## Usage

### Basic Mounting

Mount the filesystem with a storage directory for JSON files:

```bash
python3 qafs.py /mount/point -o storage=/path/to/storage
```

### Background Mounting

To run in the background (daemon mode):

```bash
python3 qafs.py /mount/point -o storage=/path/to/storage
```

To run in foreground for debugging:

```bash
python3 qafs.py /mount/point -o storage=/path/to/storage -f
```

### Creating Folders and Files

Create folders by making directories (works at any level):

```bash
# Create root-level folder (creates test_folder.json in storage)
mkdir /mount/point/test_folder

# Create nested folders (arbitrary depth supported)
mkdir /mount/point/test_folder/subfolder
mkdir /mount/point/test_folder/subfolder/deep_folder
```

Create files within folders at any nesting level:

```bash
# File in root-level folder
echo "test content" > /mount/point/test_folder/test_file.txt

# File in nested folder
echo "deep content" > /mount/point/test_folder/subfolder/deep_folder/file.txt
```

### Internet Archive Integration

Copy an Internet Archive `*_files.xml` file to the root to automatically create a folder:

```bash
# Copy the XML file to the mounted filesystem
cp la-razon-1917-2101_files.xml /mount/point/

# The folder is automatically created when the file is closed
ls /mount/point/la-razon-1917-2101/
```

**How it works:**
1. The XML file is copied to the root (buffered in memory during transfer)
2. When the file is closed, the filesystem parses the XML
3. A folder named after the identifier (`la-razon-1917-2101`) is automatically created
4. All files from the XML appear in the folder with correct sizes and timestamps

The XML parser extracts:
- File names and sizes from `<file>` elements
- Modification times from `<mtime>` child elements
- Creates nested folders if filenames contain path separators
- Preserves all metadata for testing purposes

### Control Commands

Use the `.control` file to configure the filesystem:

```bash
# Enable persistent changes (default)
echo "persistent true" > /mount/point/.control

# Disable persistent changes (ephemeral mode)
echo "persistent false" > /mount/point/.control

# Set total filesystem size (in bytes)
echo "total_size 2147483648" > /mount/point/.control  # 2GB

# Set free space (in bytes)
echo "free_size 1073741824" > /mount/point/.control   # 1GB
```

### JSON File Structure

Each root-level folder is stored as a JSON file in the storage directory.
The structure supports arbitrary nesting with both `files` and `folders` at each level:

```json
{
  "mode": 16877,
  "uid": 1000,
  "gid": 1000,
  "atime": 1695072000,
  "mtime": 1695072000,
  "ctime": 1695072000,
  "files": {
    "example.txt": {
      "size": 12,
      "mode": 420,
      "uid": 1000,
      "gid": 1000,
      "atime": 1695072000,
      "mtime": 1695072000,
      "ctime": 1695072000,
      "content_seed": 42
    }
  },
  "folders": {
    "subfolder": {
      "mode": 493,
      "uid": 1000,
      "gid": 1000,
      "atime": 1695072000,
      "mtime": 1695072000,
      "ctime": 1695072000,
      "files": {},
      "folders": {}
    }
  }
}
```

**Note:** Files don't store actual content. Instead, they use `content_seed` to generate
reproducible random data on-the-fly, allowing large file sizes without disk storage.

### Unmounting

```bash
fusermount -u /mount/point
```

## Testing Use Cases

QAFS is designed for testing file synchronization systems. You can:

1. Create test scenarios by manually editing JSON files (including nested folder structures)
2. Simulate different file sizes, timestamps, and permissions
3. Test with ephemeral changes to avoid affecting the JSON storage
4. Test deeply nested directory structures (unlimited depth)
5. Use Internet Archive `*_files.xml` metadata to quickly create realistic test datasets with multiple files
6. Simulate large files without consuming disk space (using content_seed)
7. Test file synchronization with Internet Archive items containing hundreds of files

