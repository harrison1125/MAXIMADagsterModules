"""
Batch process .MCA files using PyMCA.

This script recursively walks through the `root_dir` and processes all .MCA files
in each folder using PyMCA's batch mode. The configuration file is specified in
`Inputs.config_path`. The output is stored in the same folder as the input .MCA files.

Note
----
The PyMCA configuration file may need to have `outputconcentrations = 1` manually
set for proper output. Consider automating the generation of these config files
using MAXIMA outputs and sample data from IGSNs in the future.
"""

import subprocess
import os
import Inputs

# Config path
config_path = Inputs.config_path
root_dir = Inputs.root_dir

for dirpath, dirnames, filenames in os.walk(root_dir):
    """
    Iterate over all directories and subdirectories in `root_dir`.

    Parameters
    ----------
    dirpath : str
        Current directory path being walked.
    dirnames : list of str
        List of subdirectories in the current directory.
    filenames : list of str
        List of files in the current directory.
    """
    # Collect .MCA files only in the current folder
    mca_files = [os.path.join(dirpath, f) for f in filenames if f.lower().endswith(".mca")]

    # Run pymcabatch if there are .mca files in the folder
    if mca_files:
        print(f"Processing folder: {dirpath} with {len(mca_files)} .mca files")
        
        # Make sure output directory exists (in this case, same as folder)
        os.makedirs(dirpath, exist_ok=True)

        command = [
            "pymcabatch",
            f"--cfg={config_path}",
            f"--outdir={dirpath}",
            "--concentrations=1",
            "--exitonend=1"
        ] + mca_files

        result = subprocess.run(command, capture_output=False, text=True)

        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"pymcabatch failed in {dirpath} with error code {result.returncode}")
            print(result.stderr)
