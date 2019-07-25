#!/usr/bin/env python36

import argparse
import sys
import os
import shutil
import subprocess
import functools
from pathlib import Path
import collections
import functools
HERE = Path(os.path.dirname(os.path.abspath(__file__)))


class keydefaultdict(collections.defaultdict):
    """default dict but give key to function"""

    # https://stackoverflow.com/a/2912455/3936601
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


@functools.lru_cache(maxsize=100_000)
def dmlser(path, project):
    """get all files in path (a directory) that are on tape"""
    return dict(
        # this probably doesnt handle whitespace names correctly
        (l.split()[8], int(l.split()[4])) for l in
        subprocess.run(
            f"mdss -P {project} dmls -l".split() + [path],
            stdout=subprocess.PIPE,
            check=True,
            encoding='utf8',
        ).stdout.strip().split('\n')[1:]
        if l.split()[7] in ['(DUL)', '(OFL)']
    )

def dmls_size(path, project):
    """get size of one file (must be on tape)"""
    return dmlser(os.path.dirname(path), project)[os.path.basename(path)]

def dmls_ontape(path, project):
    """is path on tape? cached"""
    return os.path.basename(path) in dmlser(os.path.dirname(path), project)


class Job(object):
    """This class is messily tied in with main
        Just a handy state store.
    """

    def __init__(self, file, dest, project):
        self.file = Path(file)
        self.file_mdss_done = Path(str(self.file) + ".mdssok")
        self.dest = dest
        self.project = project

        self.workdir = HERE / "run" / self.file.name

        self.mdss_put = self.Step(
            'mdss_put',
            os.path.join(HERE,
                         'mdss_put.pbs.sh'),
            [
                "-q",
                "copyq",
                "-P",
                "gd7", #project,
                "-l",
                "wd",
                "-l",
                "mem=2GB",
                "-l",
                "walltime=10:00:00",
                "-l",
                "other=gdata2:gdata3:mdss",
            ],
            [file, dest, project],
            self.workdir,
        )

    class Step(object):
        """
        A job_script is run in jobdir/jobname
        When the job jid is gone from qstat the file {job_script}.ok
        indicates success. This Class stores state in jobdir in files
        job_name.start and job_name.end.
        """

        def __init__(self, job_name, job_script, qargs, jargs, jobdir):
            self.job_name = job_name
            self.job_script = job_script
            self.qargs = qargs
            self.jargs = jargs
            self.jobdir = jobdir
            self.workdir = self.jobdir / job_name
            self.warn_fail = False

            self.started_file = self.jobdir / f"{job_name}.start"
            self.done_file = self.jobdir / f"{job_name}.done"

        def started(self):
            return self.started_file.exists()

        def done(self):
            if self.started() and not self.done_file.exists():
                jid = self.started_file.read_text().strip()
                if jid not in qstat_jids:
                    if (
                        self.workdir
                        / (os.path.basename(self.job_script) + ".ok")
                    ).exists():
                        # todo check for actually complete here
                        self.done_file.touch()
                    elif not self.warn_fail:
                        self.warn_fail = True
                        print(
                            "warn, job failed?",
                            self.job_name,
                            self.workdir,
                            jid,
                            file=sys.stderr,
                        )
            return self.done_file.exists()

        def start(self):
            # (and check if done)
            # return True if started

            if self.done():
                return False

            if not self.started():
                self.workdir.mkdir(parents=True, exist_ok=True)
                self.started_file.touch()
                shutil.copy(self.job_script, self.workdir)
                job_jid = subprocess.run(
                    [
                        "qsub",
                        "-N",
                        self.job_name,
                        "-e",
                        "stderr",
                        "-o",
                        "stdout",
                        "-l"
                        "wd",
                    ]
                    + self.qargs
                    + ["--", "bash", os.path.basename(self.job_script)]
                    + self.jargs,
                    cwd=self.workdir,
                    stdout=subprocess.PIPE,
                    check=True,
                    encoding="utf8",
                ).stdout.strip()
                self.started_file.write_text(job_jid + '\n')
                qstat_jids.add(job_jid)
                print(self.job_name, self.workdir, job_jid)
                return True

            return False

    def check_tape(self):
        # weird logic, dont want to check size until it is on tape
        # and then only once, to make sure we dont spam the system.
        if dmls_ontape(self.dest, self.project):
            assert dmls_size(self.dest, self.project) == Path(self.file).stat().st_size
            return True
        return False

def count_jobs():
    j = subprocess.run(
            "qstat -u eb8858".split(),
            stdout=subprocess.PIPE,
            check=True,
            encoding="utf8",
        )
    return set(l.strip().split()[0] for l in j.stdout.split('\n') if "mdss_put" in l)


def main(args):
    """
    it is probably better to put bunches of files in together,
    or even whole directories
    """
    put_running = len(qstat_jids)

    jobs_raw = [l.strip().split('\t') for l in args.infile]
    assert all(len(j) == 3 for j in jobs_raw)
    assert not any(any(' ' in a for a in j) for j in jobs_raw)

    jobs = [Job(*job) for job in jobs_raw]

    for job in jobs:
        assert job.file.exists() or job.file_mdss_done.exists()
        if put_running < args.put_lim:
            put_running += job.mdss_put.start()

    for job in jobs:
        if job.mdss_put.done():
            if not job.file_mdss_done.exists():
                if job.check_tape():
                    print("done:", job.file)
                    job.file_mdss_done.write_text(job.dest + '\n')
                    job.file.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="""read jobs (tsv) from {infile}.
        `filename destination project`
        eg: `/g/data/wq2/crams/AAAAA.cram results/cram/AAAAA.cram wq2`
        First a copyq mdss put job will be launched.
        Then we will wait and confirm the file is on tape and actual size.
        Finally a file will be added `{filename}.mdssok`
        and the original will be deleted.
        Progress and copyq runs are tracked in the PWD."""
    )
    parser.add_argument(
        'infile',
        type=open,
        help='infile',
        default=sys.stdin,
        nargs='?'
    )
    parser.add_argument(
        '--verbose', "-v",
        action="store_true",
    )

    def add(lim, default=1):
        parser.add_argument(
            f'--{lim}-lim',
            type=int,
            help='{lim} limit',
            default=default
        )

    add('put')

    args = parser.parse_args()

    if args.verbose:
        real_run = subprocess.run
        def verbose_run(*args, **kwargs):
            print(args, kwargs, file=sys.stderr)
            return real_run(*args, **kwargs)
        subprocess.run = verbose_run

    qstat_jids = count_jobs()
    sys.exit(main(args))
