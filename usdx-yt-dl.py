#!/usr/bin/env python3
# assumes Python>=3.10, may or may not work with older Python

import dataclasses
import glob
import itertools
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import abc
from dataclasses import dataclass
from types import ModuleType
from typing import Optional, Type, TypeVar

mutagen: Optional[ModuleType]
try:
    import mutagen.easyid3
except ImportError:
    print("mutagen is not installed, continuing without ID3 tag support...")
    mutagen = None


COMMENT_PREFIX: str = "usdx-yt-dl:"


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


def utf8_contents(path: str) -> str:
    """
    Reads the contents of the file at the given path, making a best effort to convert any non UTF8 characters.
    """
    with open(path, "rb") as fd:
        contents: bytes = fd.read()
        try:
            return contents.decode("utf-8")
        except UnicodeDecodeError:
            # Many files seem to use this encoding
            result: str = contents.decode("CP1252")
            try:
                # verify all characters are utf-8 compatible
                result.encode("utf-8")
            except UnicodeEncodeError as e:
                raise EncodingError(f"File at '{path}' contains unexpected utf-8 incompatible bytes") from e
            else:
                return result


@dataclass(frozen=True, kw_only=True)
class Metadata:
    """
    Normalized metadata.

    :ivar comment: usdb-formatted metadata comment without this tool's prefix
    :ivar video: Filename of the video. If set, must be a valid file.
    """

    title: str
    artist: str
    mp3: Optional[str] = None
    cover: Optional[str] = None
    video: Optional[str] = None
    comment: str
    video_tag: str

    @classmethod
    def from_raw_data(
        cls: Type[M],
        *,
        title: str,
        artist: str,
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
            title=title,
            artist=artist,
            mp3=mp3,
            cover=cover,
            video=normalized_video,
            comment=normalized_comment,
            video_tag=tag,
        )

    @classmethod
    def _video_tag_from_usdb_metadata(cls, tag: str) -> Optional[str]:
        def regex(*, audio_only: bool = False) -> str:
            return "(.*,)?%s=([^, \t\n\r\f\v]+)(,.*)?" % ("a" if audio_only else "v")

        # TODO: precompile all regexes
        video_match: Optional[re.Match] = re.fullmatch(regex(), tag)
        audio_match: Optional[re.Match] = re.fullmatch(regex(audio_only=True), tag)
        if video_match is not None and audio_match is not None:
            raise ConservativeSkip("Found both audio and video id, this is currently not supported")
        return video_match.group(2) if video_match is not None else audio_match.group(2) if audio_match is not None else None


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

        def get_required(field: str) -> str:
            result: Optional[str] = raw_metadata.get(field, None)
            if result is None:
                raise InsufficientData(
                    f"The metadata at '{txt_file}' does not match the expected schema"
                )
            return result

        metadata: Metadata = Metadata.from_raw_data(
            # see https://wiki.usdb.eu/txt_files/format
            title=get_required("TITLE"),
            artist=get_required("ARTIST"),
            mp3=raw_metadata.get("MP3", None),
            cover=raw_metadata.get("COVER", None),
            video=raw_metadata.get("VIDEO", None),
            comment=(
                raw_comment[len(COMMENT_PREFIX):]
                if (raw_comment := raw_metadata.get("COMMENT", None)) is not None
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
            self._set_id3_tags()
            self._fix_permissions()
            return
        if mp3_found:
            raise ConservativeSkip("Found mp3 file but no video, skipping")
        if video_found:
            raise ConservativeSkip("Found video file but no mp3, skipping")

        self._set_cover()
        self._download()
        self._set_id3_tags()
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
                ["yt-dlp", "-xk", "--audio-format", "mp3", "--", self.metadata.video_tag],
                cwd=temp_dir,
            )

            video_files: abc.Sequence[str] = [
                # account for intermediate *.f<format_id>.webm files
                *glob.iglob(os.path.join(glob.escape(temp_dir), "*].webm")),
                *glob.iglob(os.path.join(glob.escape(temp_dir), "*].mp4")),
            ]
            if len(video_files) != 1:
                raise UnknownMediaFormat(f"Expected 1 video file after download, got {len(video_files)}")
            mp3_files: abc.Sequence[str] = glob.glob(os.path.join(glob.escape(temp_dir), "*.mp3"))
            if len(mp3_files) != 1:
                raise UnknownMediaFormat(f"Expected 1 mp3 file after download, got {len(mp3_files)}")
            video_path: str = video_files[0]
            mp3_path: str = mp3_files[0]

            shutil.move(video_path, self.path)
            shutil.move(mp3_path, self.path)
            self.metadata = dataclasses.replace(
                self.metadata,
                video=os.path.basename(video_path),
                mp3=os.path.basename(mp3_path),
            )

    def _set_id3_tags(self) -> None:
        if mutagen is None:
            # ID3 tag support disabled
            return
        if self.metadata.mp3 is None:
            raise Exception("Can not set id3 tags without mp3 file present")
        path: str = os.path.join(self.path, self.metadata.mp3)
        if not os.path.exists(path):
            raise Exception(f"No mp3 file at {path}")
        mp3: mutagen.easyid3.EasyID3 = mutagen.easyid3.EasyID3(path)
        mp3["title"] = self.metadata.title
        mp3["artist"] = self.metadata.artist
        mp3["albumartist"] = ""
        mp3["album"] = "USDX library"
        mp3.save()

    def _fix_permissions(self) -> None:
        for file in os.listdir(self.path):
            if os.path.isfile(file):
                os.chmod(file, 0o640)

    def _set_raw(self, field: str, value: Optional[str]) -> None:
        if value is not None:
            self.raw_metadata[field] = value
        elif value in self.raw_metadata:
            del self.raw_metadata[field]

    def _write(self) -> None:
        self._set_raw("TITLE", self.metadata.title)
        self._set_raw("ARTIST", self.metadata.artist)
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
    if errors:
        print(f"Encountered errors for the following {len(errors)} songs:")
        for path, error in errors:
            print(f"\t{path} => {error.message}")
    if mutagen is None:
        print(
            "Skipped ID3 tagging of mp3 files because the mutagen library is not installed."
            " Simply run the tool again with mutagen installed to fix ID3 tags (media files will not be downloaded again)."
        )


if __name__ == "__main__":
    main()


# vim: tabstop=4 shiftwidth=4 expandtab:
