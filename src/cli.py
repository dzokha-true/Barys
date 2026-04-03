import cmd
import os
import shlex
import stat
import tempfile
from pathlib import Path
from typing import Any
from config import Config


class CLI(cmd.Cmd):
    intro = "Welcome to Barys :3 \nThis is a project that helps you to easily query / import your data to a SQLite database"
    prompt = "barys> "

    def __init__(
        self,
        query_service: Any,
        ingestor: Any,
        import_root: str | os.PathLike[str] | None = None,
        max_import_size_bytes: int | None = None,
    ) -> None:
        super().__init__()
        self.query_service = query_service
        self.ingestor = ingestor
        default_root = Path(__file__).resolve().parent.parent / "data"
        self.import_root = Path(import_root).expanduser().resolve(strict=False) if import_root else default_root.resolve(strict=False)
        self.max_import_size_bytes = max_import_size_bytes
        if self.max_import_size_bytes is None:
            self.max_import_size_bytes = Config().max_import_size_bytes

    def _resolve_import_path(self, filepath: str) -> Path | None:
        file_arg = filepath.strip()
        if not file_arg:
            print("Usage: import <filepath>")
            return None

        try:
            parsed_args = shlex.split(file_arg)
        except ValueError:
            print("Invalid path format. Check quotes and try again.")
            return None

        if len(parsed_args) != 1:
            print("Usage: import <filepath>")
            return None

        candidate_path = Path(parsed_args[0]).expanduser()
        if not candidate_path.is_absolute():
            candidate_path = (Path.cwd() / candidate_path)

        try:
            return candidate_path.resolve(strict=True)
        except OSError as exc:
            print(f"Could not access file '{candidate_path}': {exc}")
            return None

    # Security consideration - restricting the allowed import path to the relative path (import white-list)
    def _is_allowed_import_path(self, resolved_path: Path) -> bool:
        try:
            resolved_path.relative_to(self.import_root)
            return True
        except ValueError:
            return False

    # TOCTOU / Path safe import of a file
    def do_import(self, filepath: str) -> None:
        normalized_path = self._resolve_import_path(filepath)
        if normalized_path is None:
            return

        if not self._is_allowed_import_path(normalized_path):
            print(f"Import blocked: file must be inside '{self.import_root}'.")
            return

        open_flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"): # Ignore symlinks – only use original files (avoid permission escalation)
            open_flags |= os.O_NOFOLLOW

        try:
            file_descriptor = os.open(normalized_path, open_flags)
        except OSError as exc:
            print(f"Could not open file '{normalized_path}': {exc}")
            return

        # Snapshotting to reduce TOCTOU risks
        snapshot_path: str | None = None
        with os.fdopen(file_descriptor, "rb") as source_file:
            file_stat = os.fstat(source_file.fileno())
            if not stat.S_ISREG(file_stat.st_mode): # Ignore streaming inputs that would stall our system, only regular files
                print("Import blocked: only regular files can be imported.")
                return

            if file_stat.st_size > self.max_import_size_bytes:
                size_gb = file_stat.st_size / (1024 * 1024 * 1024)
                max_size_gb = self.max_import_size_bytes / (1024 * 1024 * 1024)
                print(
                    f"Import blocked: file is {size_gb:.2f} GB, which exceeds the {max_size_gb:.2f} GB limit."
                )
                return

            size_mb = file_stat.st_size / (1024 * 1024)
            confirmation = input(
                f"File {normalized_path} is approximately {size_mb:.2f} MB. "
                "Are you sure you want to import this into the database? (y/n) "
            ).strip().lower()

            if confirmation != "y":
                print("Import cancelled.")
                return

            # Controlled temp path to avoid TOCTOU and shell/path injection risks downstream.
            with tempfile.NamedTemporaryFile(prefix="barys_import_", suffix=".csv", delete=False) as temp_file:
                # TempFile is given a random name with a suffix and prefix – herlps against '; rm -rf /' kind of named
                source_file.seek(0)
                while True:
                    chunk = source_file.read(1024 * 1024)
                    if not chunk:
                        break
                    temp_file.write(chunk)
                snapshot_path = temp_file.name

        if snapshot_path is None:
            print("Import failed: could not create a secure file snapshot.")
            return

        try:
            self.ingestor.ingest_file(snapshot_path)
            print(f"Import completed for '{normalized_path}'.")
        except Exception as exc:
            print(f"Import failed: {exc}")
        finally:
            if snapshot_path:
                try:
                    os.remove(snapshot_path)
                except OSError:
                    pass

    def default(self, line: str) -> None:
        """Route natural-language input to query_service.process_nl_query()."""
        user_input = line.strip()
        if not user_input:
            return

        try:
            result = self.query_service.process_nl_query(user_input)
        except Exception as exc:
            print(f"Query failed: {exc}")
            return

        if result is not None:
            print(result)

    def do_help(self, arg: str) -> None:
        """Show help for commands and natural-language query usage."""
        topic = arg.strip()
        if topic:
            super().do_help(topic)
            return

        print("Available commands:")
        print("  import <filepath>   Import a CSV after confirmation")
        print("  help [command]      Show help for commands")
        print("  exit | quit         Exit the shell")
        print("  Ctrl-D              Exit the shell")
        print()
        print("Any non-command text is treated as a natural-language query.")

    def help_import(self) -> None:
        max_size_gb = self.max_import_size_bytes / (1024 * 1024 * 1024)
        print("import <filepath>")
        print("  Reads the file size, asks for confirmation, then imports via ingestor.")
        print(f"  Security limits: only regular files inside the import root, max size {max_size_gb:.2f} GB.")

    def help_exit(self) -> None:
        print("exit")
        print("  Exit the shell. You can also use 'quit' or Ctrl-D.")

    def help_quit(self) -> None:
        self.help_exit()

    def do_exit(self, _line: str) -> bool:
        return True

    def do_quit(self, _line: str) -> bool:
        return True

    def do_EOF(self, _line: str) -> bool:
        print()
        return True
