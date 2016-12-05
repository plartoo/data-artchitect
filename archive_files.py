import sys
from s3_utils import *

source_folder = sys.argv[1]     # e.g., "FilesForDatamart/Facebook/"
dest_folder = sys.argv[2]       # e.g., "Facebook/"

archive_files(source_folder, dest_folder)
