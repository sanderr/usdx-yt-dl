#!/usr/bin/env python3
# assumes Python>=3.10, may or may not work with older Python

import dataclasses
import glob
import itertools
import functools
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import abc
from dataclasses import dataclass
from typing import Optional, Type, TypeVar


COMMENT_PREFIX: str = "usbd-dl:"
UTF8_CONVERSIONS: abc.Mapping[bytes, bytes] = {
    bytes.fromhex('92'): b"'",
    bytes.fromhex('b4'): b"'",
}


M = TypeVar("M", bound="Metadata")
S = TypeVar("S", bound="Song")


class SkipException(Exception):
    """
    An exception that should cause the processing of a song to be skipped.
    """
    def __init__(self, message: str) -> None:
        self.message: str = message
        super().__init__()


class InsufficientData(SkipException):
    pass


class FileCorrupt(SkipException):
    pass


class UnexpectedState(SkipException):
    pass


class EncodingError(SkipException):
    pass


class ConservativeSkip(SkipException):
    pass


class UnknownMediaFormat(SkipException):
    pass


# TODO: UnicodeDecodeError


def utf8_contents(path: str) -> str:
    """
    Reads the contents of the file at the given path, making a best effort to convert any non UTF8 characters.
    """
    with open(path, "rb") as fd:
        contents: bytes = fd.read()
        try:
            return functools.reduce(
                lambda acc, convert: acc.replace(convert, UTF8_CONVERSIONS[convert]),
                UTF8_CONVERSIONS,
                contents,
            ).decode("utf-8")
        except UnicodeDecodeError as e:
            raise EncodingError(f"File at '{path}' contains unexpected non-utf8 bytes") from e


@dataclass(frozen=True, kw_only=True)
class Metadata:
    """
    Normalized metadata.

    :ivar comment: usdb-formatted metadata comment without this tool's prefix
    :ivar video: Filename of the video. If set, must be a valid file.
    """

    mp3: Optional[str] = None
    cover: Optional[str] = None
    video: Optional[str] = None
    comment: str
    video_tag: str

    @classmethod
    def from_raw_data(
        cls: Type[M],
        *,
        mp3: Optional[str] = None,
        cover: Optional[str] = None,
        video: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> M:
        normalized_video: Optional[str]
        normalized_comment: str
        tag: str
        if comment is not None:
            # already processed by this tool at some point
            normalized_video = video
            normalized_comment = comment
            comment_tag: Optional[str] = cls._video_tag_from_usdb_metadata(comment)
            if comment_tag is None:
                raise FileCorrupt("Found comment left by this tool but it is corrupt")
            tag = comment_tag
        else:
            # raw usdb file => extract metadata from video field
            usdb_tag_missing: InsufficientData = InsufficientData("Failed to find usdb-formatted metadata comment")
            if video is None:
                raise usdb_tag_missing
            video_tag: Optional[str] = cls._video_tag_from_usdb_metadata(video)
            if video_tag is None:
                raise usdb_tag_missing
            normalized_video = None
            normalized_comment = video
            tag = video_tag

        return cls(
            mp3=mp3,
            cover=cover,
            video=normalized_video,
            comment=normalized_comment,
            video_tag=tag,
        )

    @classmethod
    def _video_tag_from_usdb_metadata(cls, tag: str) -> Optional[str]:
        # TODO: precompile all regexes
        match: Optional[re.Match] = re.fullmatch("v=([^, \t\n\r\f\v]+)(,.*)?", tag)
        return match.group(1) if match is not None else None


class Song:
    def __init__(self, directory: str) -> None:
        self.path: str = directory
        txt_files: abc.Sequence[str] = glob.glob(os.path.join(glob.escape(self.path), "*.txt"))
        if len(txt_files) != 1:
            raise UnexpectedState(
                f"Found {len(txt_files)} txt files in '{self.path}'"
            )
        self.txt_file = txt_files[0]

        self.metadata: Metadata
        self.raw_metadata: dict[str, str]
        self.raw_body: str

        (self.metadata, self.raw_metadata, self.raw_body) = self._parse_file(self.txt_file)

    @classmethod
    def _parse_file(
        cls: Type[S], txt_file: str
    ) -> tuple[
        Metadata,
        dict[str, str],
        str,
    ]:
        contents: str = utf8_contents(txt_file)

        lines: abc.Sequence[str] = contents.splitlines()
        comment_block: abc.Sequence[str] = list(
            itertools.takewhile(lambda line: line.startswith("#"), lines)
        )
        body: str = "\n".join(lines[len(comment_block):])

        def read_line(line: str) -> tuple[str, str]:
            assert line.startswith("#")
            split: abc.Sequence[str] = line[1:].split(":", maxsplit=1)
            if len(split) != 2 or not all(split):
                raise FileCorrupt(f"Invalid metadata line: '{line}' in file '{txt_file}'")
            return (split[0], split[1])

        # TODO: this deletes comments if there is more than one
        raw_metadata: dict[str, str] = dict(read_line(line) for line in comment_block)

        def get(field: str, *, required: bool = True) -> Optional[str]:
            result: Optional[str] = raw_metadata.get(field, None)
            if result is None and required:
                raise InsufficientData(
                    f"The metadata at '{txt_file}' does not match the expected schema"
                )
            return result

        metadata: Metadata = Metadata.from_raw_data(
            # see https://wiki.usdb.eu/txt_files/format
            mp3=get("MP3"),
            cover=get("COVER", required=False),
            video=get("VIDEO", required=False),
            comment=(
                raw_comment[len(COMMENT_PREFIX):]
                if (raw_comment := get("COMMENT", required=False)) is not None
                and raw_comment.startswith(COMMENT_PREFIX)
                else None
            )
        )
        return (metadata, raw_metadata, body)

    def process(self) -> None:
        mp3_found: bool = self.metadata.mp3 is not None and os.path.exists(os.path.join(self.path, self.metadata.mp3))
        video_found: bool = self.metadata.video is not None and os.path.exists(os.path.join(self.path, self.metadata.video))
        if mp3_found and video_found:
            # both mp3 and video are set -> nothing to do here, fix permissions just in case
            self._fix_permissions()
            return
        if mp3_found:
            raise ConservativeSkip("Found mp3 file but no video, skipping")
        if video_found:
            raise ConservativeSkip("Found video file but no mp3, skipping")

        self._set_cover()
        self._download()
        self._fix_permissions()
        self._write()

    def _set_cover(self) -> None:
        jpeg_files: abc.Sequence[str] = glob.glob(os.path.join(glob.escape(self.path), "*.jpg"))
        if not jpeg_files:
            self.metadata = dataclasses.replace(self.metadata, cover=None)
        elif len(jpeg_files) == 1:
            self.metadata = dataclasses.replace(self.metadata, cover=os.path.basename(jpeg_files[0]))
        else:
            raise UnexpectedState(f"Found more than one jpeg file in '{self.path}'")

    def _download(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            subprocess.check_call(
                ["yt-dlp", "-xk", "--audio-format", "mp3", self.metadata.video_tag],
                cwd=temp_dir,
            )

            webm_files: abc.Sequence[str] = glob.glob(
                # account for intermediate *.f<format_id>.webm files
                os.path.join(glob.escape(temp_dir), "*].webm",)
            )
            if len(webm_files) != 1:
                raise UnknownMediaFormat(f"Expected 1 webm file after download, got {len(webm_files)}")
            mp3_files: abc.Sequence[str] = glob.glob(os.path.join(glob.escape(temp_dir), "*.mp3"))
            if len(mp3_files) != 1:
                raise UnknownMediaFormat(f"Expected 1 mp3 file after download, got {len(mp3_files)}")
            webm_path: str = webm_files[0]
            mp3_path: str = mp3_files[0]

            shutil.move(webm_path, self.path)
            shutil.move(mp3_path, self.path)
            self.metadata = dataclasses.replace(
                self.metadata,
                video=os.path.basename(webm_path),
                mp3=os.path.basename(mp3_path),
            )

    def _fix_permissions(self) -> None:
        for file in os.listdir(self.path):
            if os.path.isfile(file):
                os.chmod(file, 0o640)

    def _set_raw(self, field: str, value: Optional[str]) -> None:
        if value is not None:
            self.raw_metadata[field] = value
        else:
            del self.raw_metadata[field]

    def _write(self) -> None:
        self._set_raw("MP3", self.metadata.mp3)
        self._set_raw("COVER", self.metadata.cover)
        self._set_raw("VIDEO", self.metadata.video)
        self._set_raw("COMMENT", COMMENT_PREFIX + self.metadata.comment)

        metadata_text: str = "\n".join(f"#{key}:{value}" for key, value in self.raw_metadata.items())
        with open(self.txt_file, "w") as fd:
            fd.write(metadata_text + "\n" + self.raw_body)


def main() -> None:
    try:
        subprocess.check_output(["yt-dlp", "--help"])
    except subprocess.CalledProcessError:
        print("ERROR: this script requires yt-dlp")
        exit(1)

    if len(sys.argv) != 2:
        print("Usage: usdx-yt-dl.py <path-to-bulk-dir>")
        exit(1)

    all_songs_path: str = sys.argv[1]

    count: int = 0
    errors: list[tuple[str, SkipException]] = []
    for subdir in os.listdir(all_songs_path):
        song_dir: str = os.path.join(all_songs_path, subdir)
        if not os.path.isdir(song_dir):
            continue

        try:
            print(f"processing '{song_dir}'...")
            song: Song = Song(song_dir)
            song.process()
        except SkipException as e:
            errors.append((song_dir, e))
            continue

        count += 1

    print(f"Successfully processed {count} songs")
    print(f"Encountered errors for the following {len(errors)} songs:")
    for path, error in errors:
        print(f"\t{path} => {error.message}")


if __name__ == "__main__":
    main()


# vim: tabstop=4 shiftwidth=4 expandtab:
