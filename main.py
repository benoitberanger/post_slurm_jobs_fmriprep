import os
import glob

# parameters
fmriprep_image = '/network/lustre/iss02/home/benoit.beranger/fmriprep_23.0.2.simg'
main_path = '/network/lustre/iss02/cenir/analyse/irm/users/benoit.beranger/parallelize_fmriprep'

# init paths
bids_path = os.path.join(main_path, 'bids')
sub_dirs = glob.glob(os.path.join(bids_path, 'sub-*'))
assert len(sub_dirs) > 0
slurm_path = os.path.join(main_path, 'slurm')
slurm_job_path = os.path.join(slurm_path, 'job')
slurm_out_path = os.path.join(slurm_path, 'out')
slurm_err_path = os.path.join(slurm_path, 'err')
if not os.path.exists(slurm_job_path): os.makedirs(slurm_job_path)
if not os.path.exists(slurm_out_path): os.makedirs(slurm_out_path)
if not os.path.exists(slurm_err_path): os.makedirs(slurm_err_path)

# write individual jobs
jobs_dir = os.path.join(main_path, 'jobs')
for idx, sub in enumerate(sub_dirs):
    sub_name = os.path.basename(sub)
    OUT = os.path.join(main_path, 'out', sub_name)
    WORK = os.path.join(main_path, 'work', sub_name)
    print('###########################################################################################################')
    print(f'{idx+1}/{len(sub_dirs)} {sub_name} \n')
    job = f"SUB_NAME={sub_name} \n" \
          f"MAIN_PATH={main_path} \n" \
          f"BIDS={bids_path} \n" \
          f"OUT={OUT} \n" \
          f"WORK={WORK} \n" \
          f"\n" \
          f"FSDIR=/network/lustre/iss02/cenir/software/irm/freesurfer7.0_centos08/ \n" \
          f"export SINGULARITYENV_FS_LICENSE=$FSDIR/license.txt \n" \
          f"SIMG={fmriprep_image} \n" \
          f"\n" \
          f"singularity run --cleanenv -B /network/lustre/iss02/cenir/ -B $BIDS:/bids -B $OUT:/out -B $WORK:/work $SIMG /bids /out participant --participant-label {sub_name} -w /work \n"

    with open(os.path.join(slurm_job_path, f'job_{idx+1}.sh'), 'w') as fid:
        fid.write(job)
    if not os.path.exists(OUT): os.makedirs(OUT)
    if not os.path.exists(WORK): os.makedirs(WORK)

# write sbatch command
sbatch = f"#!/bin/bash \n" \
         f"#SBATCH --partition=normal,bigmem \n" \
         f"#SBATCH --time=24:00:00 \n" \
         f"#SBATCH --mem=8G \n" \
         f"#SBATCH --ntasks=1 \n" \
         f"#SBATCH --cpus-per-task=1 \n" \
         f"#SBATCH --nodes=1 \n" \
         f"#SBATCH --job-name=fmriprep_{os.path.basename(main_path)} \n" \
         f"#SBATCH --error={slurm_err_path}/err-%A_%a.txt \n" \
         f"#SBATCH --output={slurm_out_path}/out-%A_%a.txt \n" \
         f"#SBATCH --array=1-{len(sub_dirs)} \n" \
         f"\n" \
         f"echo 'SLURM_JOBID: ' $SLURM_JOBID \n" \
         f"echo 'SLURM_ARRAY_JOB_ID: ' $SLURM_ARRAY_JOB_ID \n" \
         f"echo 'SLURM_ARRAY_TASK_ID: ' $SLURM_ARRAY_TASK_ID \n" \
         f"\n" \
         f"bash {slurm_job_path}/job_${{SLURM_ARRAY_TASK_ID}}.sh \n" \

with open(os.path.join(slurm_path, 'post_slurm_jobs.sh'), 'w') as fid:
    fid.write(sbatch)

print('DONE')
