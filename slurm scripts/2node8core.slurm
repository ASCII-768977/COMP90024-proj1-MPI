#!/bin/bash
# Created by the University of Melbourne job script generator for SLURM
# Sat Apr 04 2020 19:20:33 GMT+1100

# Partition for the job:
#SBATCH --partition=cloud

# Multithreaded (SMP) job: must run on one node and the cloud partition
#SBATCH --nodes=2

# The name of the job:
#SBATCH --job-name="2n8c-test"

# The project ID which this job should run under:
#SBATCH --account="COMP90024"

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

# Generate output
#SBATCH --output=2node8core.out



# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# begins
#SBATCH --mail-type=BEGIN
# ends successfully
#SBATCH --mail-type=END

# Use this email address:
#SBATCH --mail-user=yumingl@student.unimelb.edu.au

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=1-0:0:00

# check that the script is launched with sbatch
if [ "x$SLURM_JOB_ID" == "x" ]; then
   echo "You need to submit your job to the queuing system with sbatch"
   exit 1
fi

# Run the job from this directory:
cd /home/$USER/comp90024-proj1/

# The modules to load:
module load Python/3.7.3-spartan_gcc-8.1.0

# The job command(s):
time mpirun python3 proj.py