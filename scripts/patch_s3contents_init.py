"""
Patch s3contents to work with prefix-restricted MinIO policies.

Problem: s3contents calls mkdir("") during initialization which fails when users
only have access to specific prefixes (not the bucket root).

Solution: Patch the init() method to be lenient - skip mkdir if it fails with
permission errors, but continue with normal operation.
"""

import os
import sys
import re

TARGET_FILE = "/opt/conda/lib/python3.13/site-packages/s3contents/s3_fs.py"


def patch_s3fs_init():
    if not os.path.exists(TARGET_FILE):
        print(f"Error: Target file {TARGET_FILE} not found.")
        sys.exit(1)

    print(f"Patching {TARGET_FILE} for prefix-restricted policies...")

    with open(TARGET_FILE, "r") as f:
        content = f.read()

    # The original init() method exits on AccessDenied.
    # We want to make it lenient - skip mkdir but continue.

    if "# BERDL patch" in content:
        print("File is already patched.")
        return

    # Look for the init method and patch it differently
    if "def init(self):" in content and "self.mkdir" in content:
        # Find and patch just the mkdir line to catch errors
        # The line usually looks like '        self.mkdir("")' (8 spaces)

        # We need to find the exact indentation of the self.mkdir("") call
        # match group 1 is the indentation string (spaces/tabs)
        mkdir_match = re.search(r'([ \t]*)self\.mkdir\(""\)', content)

        if mkdir_match:
            original_line = mkdir_match.group(0)
            indent = mkdir_match.group(1)

            # Create replacement with correct indentation relative to the found line
            # We add one extra level of indentation (4 spaces) for the code inside try
            # NOTE: We use strict 4 spaces for inner indent
            new_block = f"""{indent}try:
{indent}    self.mkdir("")
{indent}except (ClientError, PermissionError) as ex:
{indent}    self.log.warning(f"Could not create .s3keep marker: {{ex}}")  # BERDL patch"""

            new_content = content.replace(original_line, new_block, 1)

            with open(TARGET_FILE, "w") as f:
                f.write(new_content)
            print("Successfully applied patch to s3_fs.py")
        else:
            print('Error: Could not find self.mkdir("") pattern to patch.')
            sys.exit(1)
    else:
        print("Error: init() method not found in expected format.")
        sys.exit(1)


if __name__ == "__main__":
    patch_s3fs_init()
