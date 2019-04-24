from pymongo import MongoClient
import gridfs
import os


class FS:
    def __init__(self, db_host: str, db_port: int, username: str, password: str, db: str):
        """
            Arguments:
                db_host: str    -host-ip or host-name of the mongo database
                db_port: int    -port number of the mongo database
                username: str   -Username of the mongo database
                password: str   -Password for the user account
                db: str  -database; Name of the database assigned to the user

            Action:
                Creates a containers object for the user

            Return:
                Returns an object of the class Containers
        """
        self.username = username
        self.password = password
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[db]
        self.fs = gridfs.GridFS(self.db)
        self.fsb = gridfs.GridFSBucket(self.db)
        self.working_directory = '/'

    def __enter__(self, db_host: str, db_port: int, username: str, password: str, db: str):
        """
            Arguments:
                db_host: str    -host-ip or host-name of the mongo database
                db_port: int    -port number of the mongo database
                username: str   -Username of the mongo database
                password: str   -Password for the user account
                db: str  -database; Name of the database assigned to the user

            Action:
                Creates a containers object for the user

            Return:
                Returns an object of the class Containers
        """
        self.username = username
        self.password = password
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[db]
        self.fs = gridfs.GridFS(self.db)
        self.working_directory = '/'

        if not self.fs.find_one('/.'):
            self.fs.put(b'', file_name='/.')

    def parse(self, path):
        path = os.path.join(self.working_directory, path, '.')
        working_directory = self.working_directory
        for segment in path.split('/')[1:]:
            if segment == '.':
                continue
            elif segment == '..':
                working_directory = os.path.dirname(working_directory)
            else:
                working_directory = os.path.join(working_directory, segment)
        return working_directory

    def is_dir(self, path):
        path = os.path.join(path, '.')
        return self.fs.exists({'file_name': path})

    def ls(self):
        files = self.fs.find({'file_name': {'$regex': rf'{self.working_directory}[0-9A-Za-z_\-\.]+'}})
        return [file.file_name for file in files]

    def pwd(self):
        return self.working_directory

    def mkdir(self, path):
        path = self.parse(path)
        if self.is_dir(path):
            raise AssertionError('Directory already exists')
        self.fs.put(b'', file_name=os.path.join(path, '.'))

    def rm(self, path):
        path = self.parse(path)
        if self.is_dir(path):
            raise AssertionError('Path is directory')

        file = self.fs.find_one({'file_name': path})
        if not file:
            raise AssertionError('File does not exist')
        self.fs.delete(file._id)

    def rmdir(self, path):
        path = self.parse(path)
        if not self.is_dir(path):
            raise AssertionError('Path is not a directory')

        files = self.fs.find({'file_name': {'$regex': rf'{path}/.*'}})
        for file in files:
            self.fs.delete(file._id)

    def cd(self, path):
        path = self.parse(path)
        if not self.is_dir(path):
            raise AssertionError('Path is not a directory')
        self.working_directory = path + '/'

    def cp(self, source_path, destination_path):
        source_path = self.parse(source_path)
        destination_path = self.parse(destination_path)

        if self.is_dir(source_path):
            sources = self.fs.find({'file_name': {'$regex': rf'{source_path}/.*'}})
        else:
            sources = [self.fs.find_one({'file_name': source_path})]

        if not sources:
            raise AssertionError('Source path does not exist')

        if self.is_dir(destination_path) or self.fs.exists(destination_path):
            raise AssertionError('Destination path already exists')

        destination_parent = os.path.dirname(destination_path)
        if not self.is_dir(destination_parent):
            raise AssertionError('Destination parent does not exist')

        for source in sources:
            suffix = os.path.relpath(source.file_name, source_path)
            self.fs.put(source.read(), file_name=os.path.join(destination_path, suffix))

    def mv(self, source_path, destination_path):
        source_path = self.parse(source_path)
        destination_path = self.parse(destination_path)

        if self.is_dir(source_path):
            sources = self.fs.find({'file_name': {'$regex': rf'{source_path}/.*'}})
        else:
            sources = [self.fs.find_one({'file_name': source_path})]

        if not sources:
            raise AssertionError('Source path does not exist')

        if self.is_dir(destination_path) or self.fs.exists(destination_path):
            raise AssertionError('Destination path already exists')

        destination_parent = os.path.dirname(destination_path)
        if not self.is_dir(destination_parent):
            raise AssertionError('Destination parent does not exist')

        for source in sources:
            suffix = os.path.relpath(source.file_name, source_path)
            self.fsb.rename(source._id, os.path.join(destination_path, suffix))

    def get(self, source_path, destination_path):
        source_path = self.parse(source_path)
        destination_path = self.parse(destination_path)

        if self.is_dir(source_path):
            sources = self.fs.find({'file_name': {'$regex': rf'{source_path}/[0-9A-Za-z_\-\.]+'}})
        else:
            sources = [self.fs.find_one({'file_name': source_path})]

        if not sources:
            raise AssertionError('Source path does not exist')

        if os.path.exists(os.path.join(destination_path, '.')) or os.path.exists(destination_path):
            raise AssertionError('Destination path already exists')

        destination_parent = os.path.dirname(destination_path)
        if not self.is_dir(destination_parent):
            raise AssertionError('Destination parent does not exist')

        for source in sources:
            suffix = os.path.relpath(source.file_name, source_path)
            if self.is_dir(source.file_name):
                continue
            with open(suffix, 'wb') as f:
                f.write(source.read())

    def put(self, source_path, destination_path):
        source_path = self.parse(source_path)
        destination_path = self.parse(destination_path)

        if not os.path.exists(os.path.join(destination_path, '.')) or not os.path.exists(destination_path):
            raise AssertionError('Source path does not exist')

        if self.is_dir(destination_path) or self.fs.exists(destination_path):
            raise AssertionError('Destination path already exists')

        destination_parent = os.path.dirname(destination_path)
        if not self.is_dir(destination_parent):
            raise AssertionError('Destination parent does not exist')

        sources = os.listdir(source_path)

        for source in sources:
            if os.path.isdir(source):
                continue
            with open(source, 'rb') as f:
                data = f.read()
            suffix = os.path.relpath(source, source_path)
            self.fs.put(data, file_name=os.path.join(destination_path, suffix))

    def close(self) -> None:
        """
            Arguments:

            Action:
                Closes the database connection client
            Return:
                None
        """
        self.client.close()

    def __exit__(self) -> None:
        """
            Arguments:

            Action:
                Closes the database connection client
            Return:
                None
        """
        self.client.close()
