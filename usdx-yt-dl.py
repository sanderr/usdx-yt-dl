#!/usr/bin/env python3
# assumes Python>=3.10, may or may not work with older Python

import contextlib
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
    print("Warning: mutagen is not installed, continuing without ID3 tag support...")
    mutagen = None

RSGAIN: bool
if shutil.which("rsgain") is not None:
    RSGAIN = True
else:
    print("Warning: rsgain is not installed, continuing without replaygain tagging...")
    RSGAIN = False


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


class DownloadFailed(SkipException):
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
    background: Optional[str] = None
    video: Optional[str] = None
    comment: str
    video_tag: Optional[str]
    audio_tag: Optional[str]

    @classmethod
    def from_raw_data(
        cls: Type[M],
        *,
        title: str,
        artist: str,
        mp3: Optional[str] = None,
        cover: Optional[str] = None,
        background: Optional[str] = None,
        video: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> M:
        normalized_video: Optional[str]
        normalized_comment: str
        if comment is not None:
            # already processed by this tool at some point
            normalized_video = video
            normalized_comment = comment
        else:
            # raw usdb file => extract metadata from video field
            normalized_video = None
            normalized_comment = video
        tags: tuple[Optional[str], Optional[str]] = cls._media_tags_from_usdb_metadata(normalized_comment)
        if all(tag is None for tag in tags):
            raise InsufficientData("Failed to find usdb-formatted video or audio tags")

        return cls(
            title=title,
            artist=artist,
            mp3=mp3,
            cover=cover,
            background=background,
            video=normalized_video,
            comment=normalized_comment,
            video_tag=tags[0],
            audio_tag=tags[1],
        )

    @classmethod
    def _media_tags_from_usdb_metadata(cls, tag: str) -> tuple[Optional[str], Optional[str]]:
        """
        Returns a tuple of video and audio tags.
        """

        def regex(*, audio_only: bool = False) -> str:
            return "(.*,)?%s=([^, \t\n\r\f\v]+)(,.*)?" % ("a" if audio_only else "v")

        video_match: Optional[re.Match] = re.fullmatch(regex(), tag)
        audio_match: Optional[re.Match] = re.fullmatch(regex(audio_only=True), tag)
        return tuple(match.group(2) if match is not None else None for match in (video_match, audio_match))


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
            background=raw_metadata.get("BACKGROUND", None),
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
        files: tuple[Optional[str], Optional[str]] = (self.metadata.mp3, self.metadata.video)
        outdated: bool = any(
            filename is not None and f" [{tag}]." not in filename
            for filename, tag in zip(
                files, (
                    self.metadata.audio_tag if self.metadata.audio_tag is not None else self.metadata.video_tag,
                    self.metadata.video_tag,
                )
            )
        )
        if outdated:
            # clean up old files
            for filename in files:
                if filename is not None:
                    with contextlib.suppress(FileNotFoundError):
                        os.remove(os.path.join(self.path, filename))
        else:
            mp3_found, video_found = tuple(
                filename is not None
                and os.path.exists(os.path.join(self.path, filename))
                for filename in files
            )
            if mp3_found and (video_found or self.metadata.video is None):
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
            self.metadata = dataclasses.replace(self.metadata, cover=None, background=None)
        elif len(jpeg_files) == 1:
            file: str = os.path.basename(jpeg_files[0])
            self.metadata = dataclasses.replace(self.metadata, cover=file, background=file)
        else:
            raise UnexpectedState(f"Found more than one jpeg file in '{self.path}'")

    def _download(self) -> None:
        if self.metadata.video_tag is None and self.metadata.audio_tag is None:
            raise InsufficientData("No video or audio source found")
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                same_audio: bool = self.metadata.audio_tag is None or self.metadata.audio_tag == self.metadata.video_tag
                if self.metadata.video_tag is not None:
                    subprocess.check_call(
                        [
                            "yt-dlp",
                            *(
                                ["--extract-audio", "--keep-video", "--audio-format", "mp3"]
                                if same_audio
                                else []
                            ),
                            "--",
                            self.metadata.video_tag
                        ],
                        cwd=temp_dir,
                    )
                if not same_audio:
                    assert self.metadata.audio_tag is not None
                    subprocess.check_call(
                        ["yt-dlp", "--extract-audio", "--audio-format", "mp3", "--", self.metadata.audio_tag],
                        cwd=temp_dir,
                    )
            except subprocess.CalledProcessError:
                raise DownloadFailed("Something went wrong during download")

            video_files: abc.Sequence[str] = [
                # account for intermediate *.f<format_id>.webm files
                *glob.iglob(os.path.join(glob.escape(temp_dir), "*].webm")),
                *glob.iglob(os.path.join(glob.escape(temp_dir), "*].mp4")),
            ]
            if self.metadata.video_tag is not None and len(video_files) != 1:
                raise UnknownMediaFormat(f"Expected 1 video file after download, got {len(video_files)}")
            mp3_files: abc.Sequence[str] = glob.glob(os.path.join(glob.escape(temp_dir), "*.mp3"))
            if len(mp3_files) != 1:
                raise UnknownMediaFormat(f"Expected 1 mp3 file after download, got {len(mp3_files)}")


            mp3_path: str = mp3_files[0]
            self._rsgain(mp3_path)
            shutil.move(mp3_path, self.path)

            video_name: Optional[str]
            if self.metadata.video_tag is not None:
                video_path: str = video_files[0]
                shutil.move(video_path, self.path)
                video_name = os.path.basename(video_path)
            else:
                video_name = None

            self.metadata = dataclasses.replace(
                self.metadata,
                video=video_name,
                mp3=os.path.basename(mp3_path),
            )

    @classmethod
    def _rsgain(cls, mp3: str) -> None:
        if not RSGAIN:
            return
        subprocess.check_call(
            [
                "rsgain",
                "custom",
                "--tagmode=i",
                "--clip-mode=p",
                mp3,
            ],
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
        mp3["albumartist"] = "Various Artists"
        mp3["album"] = "USDX library"
        delete = ("date", "tracknumber")  # date should be the same for the entire album
        for d in delete:
            if d in mp3:
                del mp3[d]
        mp3.save()

    def _fix_permissions(self) -> None:
        for file in os.listdir(self.path):
            file_path: str = os.path.join(self.path, file)
            if os.path.isfile(file_path):
                os.chmod(file_path, 0o640)

    def _set_raw(self, field: str, value: Optional[str]) -> None:
        if value is not None:
            self.raw_metadata[field] = value
        elif field in self.raw_metadata:
            del self.raw_metadata[field]

    def _write(self) -> None:
        self._set_raw("TITLE", self.metadata.title)
        self._set_raw("ARTIST", self.metadata.artist)
        self._set_raw("MP3", self.metadata.mp3)
        self._set_raw("COVER", self.metadata.cover)
        self._set_raw("BACKGROUND", self.metadata.background)
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
