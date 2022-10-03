import typing
import subprocess
import sys
import enum
from dataclasses import dataclass, field, asdict
from contextlib import suppress
from datetime import datetime
import click
import re


class Release(enum.IntEnum):
    alpha=0
    beta=1
    release_candidate=2
    release=3

    @classmethod
    def from_code(cls, code: str):
        if code == 'a':
            return cls.alpha
        if code == 'b':
            return cls.beta
        if code == 'c':
            return cls.release_candidate
        return cls.release

    @classmethod
    def from_word(cls, word: str):
        try:
            return {'alpha': cls.alpha,
                    'beta': cls.beta,
                    'rc': cls.release_candidate,
                    'release': cls.release}[word]
        except KeyError:
            raise ValueError(f'unknown kind of release "{word}"')

    @staticmethod
    def to_code(r: 'Release') -> str:
        if r == Release.alpha:
            return 'a'
        elif r == Release.beta:
            return 'b'
        elif r == Release.release_candidate:
            return 'c'
        return ''


class WrongBranch(Exception):
    
    def __init__(self, required_branch: str, current_branch: typing.Optional[str]=None):
        self.required_branch = str(required_branch)
        self.current_branch = str(current_branch) if current_branch else '(unkown)'
        super().__init__(f'expected repository checked out to branch "{self.required_branch}" '
                         f'but instead it is in "{self.current_branch}"')


@dataclass(order=True)
class Version:
    major: int=0
    minor: int=0
    revision: int=0
    kind: Release=Release.release
    prerelease: int=0
    distance: int=0
    hash: typing.Optional[str]=None
    dirty: bool=False

    @classmethod
    def from_str(cls, txt: str):
        raw_ver, distance, hash, *_ = txt.strip().split('-')
        match = re.match('^v?(\d+)\.(\d+)(?:\.(\d+))?(?:(a|b|rc)([0-9]+))?$', raw_ver)
        if not match:
            raise ValueError(f'unable to interpret version string "{raw_ver}"')
        
        def _c(s) -> int:
            if s is None:
                return 0
            if s.isdigit():
                return int(s)
            return Release.from_code(s)
        
        v = (_c(s) for s in match.groups())
        return cls(*v, int(distance), hash, dirty=len(_) > 0)

    @classmethod
    def from_git(cls, require_branch: typing.Optional[str]=None):
        ret = subprocess.run(['git', 'symbolic-ref', '--short', 'HEAD'],
                             capture_output=True, check=True, text=True)
        current_branch = str(ret.stdout.strip())
        if require_branch and current_branch != require_branch:
            raise WrongBranch(require_branch, current_branch)
        ret = subprocess.run(['git', 'describe', '--tags', '--long', '--dirty'], 
                             capture_output=True, check=True, text=True)
        return cls.from_str(str(ret.stdout))

    def __str__(self) -> str:
        ver = [self.version_string()]
        if self.distance > 0 or self.dirty:
            ver.append(self.build_string())
        return '+'.join(ver)

    def version_string(self) -> str:
        ver = '.'.join(str(v) for v in (self.major, self.minor, self.revision))
        if self.kind != Release.release:
            ver += Release.to_code(self.kind) + str(self.prerelease)
        return ver
    
    def build_string(self) -> str:
        ver = []
        if self.distance > 0:
            ver.append(f'dev{self.distance:d}')
            ver.append(str(self.hash) if self.hash else '')
        if self.dirty:
            ver.append(datetime.now().strftime('d%Y%m%d'))
        return '.'.join(ver)

    def bump(self, major: bool=False, minor: bool=False, revision: bool=False, kind: typing.Optional[Release]=None, **kwargs):
        
        if kind is None:
            kind = self.kind if self.kind < Release.release else Release.alpha

        if kind > self.kind:
            return self.copy(kind=kind, prerelease=1)
        
        if self.kind == kind:
            return self.copy(prerelease=self.prerelease + 1)
        
        if major:
            return Version(self.major + 1, 0, 0, kind=kind, prerelease=1)
        if minor:
            return Version(self.major, self.minor + 1, 0, kind=kind, prerelease=1)
        if revision:
            return self.copy(revision=self.revision + 1, kind=kind, prerelease=1)

        raise ValueError('must specify either major, minor or revision to go to lower release status')

    def copy(self, *, include_hash: bool=False, **kwargs):
        current = asdict(self)
        current.update(kwargs)
        if not include_hash:
            with suppress(KeyError):
                for key in ('hash', 'distance', 'dirty'):
                    current.pop(key)
        return Version(**current)

        
def test():
    v = [Version(0, 0, 0, kind=Release.release)]
    for kwargs in (dict(minor=True), dict(minor=True), {}, dict(kind=Release.beta), 
                   dict(kind=Release.release, major=True), dict(minor=True), dict(kind=Release.release),
                   dict(kind=Release.beta, major=True), dict(kind=Release.release)):
        v.append(v[-1].bump(**kwargs))
        assert v[-1] > v[-2]

    v = Version(0, 1, 0, kind=Release.alpha, prerelease=7, distance=3, hash='g42eac42', dirty=True)
    d = datetime.now().strftime('d%Y%m%d')
    assert str(v) == f'0.1.0a7+dev3.g42eac42.{d}'


@click.group()
def main():
    pass


@main.command()
@click.option('--version-only', is_flag=True)
@click.option('--build-string-only', is_flag=True)
def version(version_only=False, build_string_only=False):
    """Display the current version of the code.

    This uses conda-buildstrings to provide additional information
    in case the code does not correspond exactly to a known git tag.
    It follows the same convetion as `setuptools_scm`, meaning.
    """
    v = Version.from_git()
    if version_only:
        print(v.version_string())
    elif build_string_only:
        print(v.build_string())
    else:
        print(v)


@main.command()
@click.option('--major', '-M', is_flag=True)
@click.option('--minor', '-m', is_flag=True)
@click.option('--revision', '-r', is_flag=True)
@click.option('--alpha', '-a', is_flag=True)
@click.option('--beta', '-b', is_flag=True)
@click.option('--rc', '-c', is_flag=True)
@click.option('--release', '-R', is_flag=True)
@click.option('--allow-dirty', '-w', is_flag=True)
@click.option('--dry-run', is_flag=True)
@click.option('--no-push', is_flag=True)
def bump(major, minor, revision, alpha, beta, rc, release, allow_dirty, dry_run, no_push):
    """Tag current commit with next version in line.

    This script will (1) determine the next version in line by the
    provided argument(s) (see below), (2) tag the current revision
    as 'v<result-of-1>' and (3) push the result to 'origin'.

    Note: must be called on `main` to help keep the code history
    clean. 
    
    Also note: pushing tags might trigger build actions on GitHub.

    How versionning works: the basis for versionning is the release
    status. It can be `alpha`, `beta`, `release candidate` or `rc`
    or `release`. The idea is that a version, say 0.1.0, goes through
    each of the four states, where only the last is called 0.1.0.
    Before, there are a set of pre-releases, which can be either
    alpha, beta or rc releases. Pre-releases of the same class can
    be further distinguished by pre-release numbers. Meaning e.g.
    there could e.g. be a 0.1.0a1, followed by 0.1.0b1 and b2. In
    other words, we consider:
    0.1.0 > 0.1.0rc1 > 0.1.0b1 > 0.1.0a2 > 0.1.0a1

    How version bumping works: 
        * no arguments: the pre-release number increases, meaning
            e.g. 0.1.0a1 becomes 0.1.0a2, and 0.1.0b1 becomes 0.1.0b2.
        * to jump to the next release status, use the corresponding flag
            e.g. calling `--beta` on 0.1.0a3 will produce 0.1.0b1. You can
            skip states, i.e. from alpha to rc or even release. In each case,
            the pre-release number will reset to 1.
        * calling `--release` removes the pre-release flags. Releases have no
            more pre-release flage, maining there can only be one.
        * calling with a release status inferior to the current one, e.g.
            going from 'release' to 'alpha', is considered as jumping to the
            next version. Therefore one of `--major`, `--minor` or `--revision`
            must be specified to let the system know which one to increase.
            E.g. calling `--alpha --revision` on 0.1.0 will produce 0.1.1a1.
        * calling `--major`, `--minor` or `--revision` without a specific
            release state is implicitly considered as a return to `--alpha`.

    The `--dry-run` allows peeking ahead to the next version, i.e. the next version
    is calculated but no tag is made or push. `--no-push` only creates the tag locally.
    """
    if alpha:
        kind = Release.alpha
    elif beta:
        kind = Release.beta
    elif rc:
        kind = Release.release_candidate
    elif release:
        kind = Release.release
    else:
        kind = None

    try:
        v = Version.from_git(require_branch='main')
    except WrongBranch as e:
        print(f'ERROR: the checked out branch is "{e.current_branch}"; '
              'new version tags may only be created in "main".')
        print('Hint: check out main')
        sys.exit(1)

    if not allow_dirty and v.dirty:
        print('ERROR: cannot bump version as long as there are uncommitted changes')
        print('Hint: the `--allow-dirty` switch allows to skip this check')
        sys.exit(1)
    v = v.bump(major=major, minor=minor, revision=revision, kind=kind)
    d = datetime.now()
    if not dry_run:
        subprocess.run(['git', 'tag', '-a', f'v{v}', f'-m "version {v} created on {d}"'])
        if not no_push:
            subprocess.run(['git', 'push', 'origin', f'v{v}'])
    print(v)
    

if __name__ == '__main__':
    main()