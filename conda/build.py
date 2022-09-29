import typing
from distutils.version import StrictVersion
import subprocess
import sys
import enum
from dataclasses import dataclass, field, asdict
from contextlib import suppress
from datetime import datetime
import click


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
        v = StrictVersion(raw_ver[1:] if raw_ver.startswith('v') else raw_ver)
        if v.prerelease is None:
            pre = (Release.release, 0)
        elif len(v.prerelease) == 1:
            pre = (Release.from_code(v.prerelease[0]), 0)
        else:
            pre = (Release.from_code(v.prerelease[0]), int(v.prerelease[1]))
        return cls(*v.version, *pre, int(distance), hash, dirty=len(_) > 0)

    @classmethod
    def from_git(cls):
        ret = subprocess.run(['git', 'describe', '--tags', '--long', '--dirty'], capture_output=True, check=True, text=True)
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
def version():
    v = Version.from_git()
    print(v)


@main.command()
@click.option('--major', '-M', is_flag=True)
@click.option('--minor', '-m', is_flag=True)
@click.option('--revision', '-r', is_flag=True)
@click.option('--alpha', '-a', is_flag=True)
@click.option('--beta', '-b', is_flag=True)
@click.option('--rc', '-c', is_flag=True)
@click.option('--release', '-R', is_flag=True)
@click.option('--allow-dirty', '-d', is_flag=True)
def bump(major, minor, revision, alpha, beta, rc, release, allow_dirty):
    
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

    v = Version.from_git()
    if not allow_dirty and v.dirty:
        print('ERROR: cannot bump version as long as there are uncommitted changes')
        print('Hint: the `--allow-dirty` switch allows to skip this check')
        sys.exit(1)
    v = v.bump(major=major, minor=minor, revision=revision, kind=kind)
    d = datetime.now()
    subprocess.run(['git', 'tag', '-a', f'v{v}', f'-m "version {v} created on {d}"'])
    subprocess.run(['git', 'push', 'origin', f'v{v}'])
    print(v)


if __name__ == '__main__':
    main()