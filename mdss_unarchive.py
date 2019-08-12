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
    """cache a dmls call (linesplit only)"""
    return (
        subprocess.run(
            f"mdss -P {project} dmls -l".split() + [path],
            stdout=subprocess.PIPE,
            check=True,
            encoding="utf8",
        )
        .stdout.strip()
        .split("\n")[1:]
    )


@functools.lru_cache(maxsize=100_000)
def dmlser_disk(path, project):
    return set(
        # this probably doesnt handle whitespace names correctly
        l.split()[8]
        for l in dmlser(path, project)
        # todo add the disk only thing here
        if l.split()[7] in ["(DUL)"]
    )


@functools.lru_cache(maxsize=100_000)
def dmlser_size(path, project):
    return dict(
        # this probably doesnt handle whitespace names correctly
        (l.split()[8], int(l.split()[4]))
        for l in dmlser(path, project)
    )


def dmls_size(path, project):
    """get size of one file"""
    return dmlser_size(os.path.dirname(path), project)[os.path.basename(path)]


def dmls_ondisk(path, project):
    """is path on disk? cached"""
    return os.path.basename(path) in dmlser_disk(
        os.path.dirname(path), project
    )


class Job(object):
    """This class is messily tied in with main
        Just a handy state store.
    """

    def __init__(
        self, mdss_file, gdata_dest_dir, mdss_project, job_project=None
    ):
        self.mdss_file = mdss_file
        assert not os.path.isabs(self.mdss_file)

        self.gdata_dest_dir = gdata_dest_dir
        assert os.path.isabs(self.gdata_dest_dir)
        assert os.path.isdir(self.gdata_dest_dir) or not os.path.exists(
            self.gdata_dest_dir
        )

        self.gdata_file = os.path.join(
            self.gdata_dest_dir, os.path.basename(self.mdss_file)
        )

        self.mdss_project = mdss_project
        self.job_project = job_project or mdss_project

        # todo name collisions!
        self.workdir = HERE / "run" / os.path.basename(self.mdss_file)

        self.mdss_get = self.Step(
            "mdss_get",
            os.path.join(HERE, "mdss_get.pbs.sh"),
            [
                "-q",
                "copyq",
                "-P",
                self.job_project,
                "-l",
                "mem=2GB",
                "-l",
                "walltime=10:00:00",
                "-l",
                "other=gdata2:gdata3:mdss",
            ],
            [self.mdss_file, self.gdata_file, self.mdss_project],
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
                        "-l",
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
                self.started_file.write_text(job_jid + "\n")
                qstat_jids.add(job_jid)
                print(self.job_name, self.workdir, job_jid)
                return True

            return False


def count_jobs():
    j = subprocess.run(
        "qstat -u eb8858".split(),
        stdout=subprocess.PIPE,
        check=True,
        encoding="utf8",
    )
    return set(
        l.strip().split()[0] for l in j.stdout.split("\n") if "mdss_get" in l
    )


def main(args):
    get_running = len(qstat_jids)

    jobs_raw = [l.strip().split("\t") for l in args.infile]
    assert all(len(j) in [3, 4] for j in jobs_raw)
    assert not any(any(" " in a for a in j) for j in jobs_raw)

    jobs = [Job(*job) for job in jobs_raw]

    for job in jobs:
        if job.mdss_get.done():
            assert os.path.exists(job.gdata_file)

    staging = collections.defaultdict(list)
    staging_total_filesize = 0
    for job in jobs:
        if not job.mdss_get.started():
            if dmls_ondisk(job.mdss_file, job.mdss_project):
                if get_running < args.get_lim:
                    get_running += job.mdss_get.start()
            else:
                if (
                    staging_total_filesize
                    + dmls_size(job.mdss_file, job.mdss_project)
                    < args.staging_lim
                ):
                    staging[job.mdss_project].append(job.mdss_file)
                    staging_total_filesize += dmls_size(
                        job.mdss_file, job.mdss_project
                    )

    # only stage more if our copyq is not full
    # this makes it a bit slower - but nicer behaviour.
    if get_running >= args.get_lim:
        for proj, files in staging.items():
            print("staging", proj)
            subprocess.run(
                f"xargs -r0 mdss -P {proj} stage".split(),
                check=True,
                input="\0".join(files),
                encoding="utf8",
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""read jobs (tsv) from {infile}.
        `mdss_filename gdata_destination mdss_project job_project`
        eg: `results/cram/AAAAA.cram /g/data/wq2/crams/AAAAA.cram wq2 tx70`
        """
    )
    parser.add_argument(
        "infile", type=open, help="infile", default=sys.stdin, nargs="?"
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    def add(lim, default=1):
        parser.add_argument(
            f"--{lim}-lim", type=int, help="{lim} limit", default=default
        )

    add("get")
    add("staging")

    args = parser.parse_args()

    if args.verbose:
        real_run = subprocess.run

        def verbose_run(*args, **kwargs):
            print(args, kwargs, file=sys.stderr)
            return real_run(*args, **kwargs)

        subprocess.run = verbose_run

    qstat_jids = count_jobs()
    sys.exit(main(args))
