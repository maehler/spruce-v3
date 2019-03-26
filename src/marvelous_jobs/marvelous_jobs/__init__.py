import pkg_resources

from marvelous_jobs.database import marvel_db
from marvelous_jobs.config import marvelous_config
from marvelous_jobs.job import daligner_job_array
from marvelous_jobs.job import check_job
from marvelous_jobs.job import merge_job_array
from marvelous_jobs.job import annotate_job_array
from marvelous_jobs.job import annotation_merge_job
from marvelous_jobs.job import repeat_annotation_array
from marvelous_jobs.job import patch_job_array
from marvelous_jobs.job import stats_job_array
from marvelous_jobs.job import masking_server_job
from marvelous_jobs.job import prepare_job

__version__ = pkg_resources.require('marvelous_jobs')[0].version
