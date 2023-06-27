# tags2sqlite
Python script leveraging puddletag codebase to import/export tag metadata to/from a dynamically created SQLite table.  Requires Python 3.x


Currently all I've done is modded the Python 2.x code to run under Python 3.

At present must be started in root of tree you intend to import.  Strongly suggest writing db to /tmp as it's dynamically modified every time a new tag is encounted in a file being imported.
