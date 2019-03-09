from pymongo import MongoClient, ReturnDocument
import gridfs
import uuid

containers = None


class Containers:
    def __init__(self, db_host: str, db_port: int, username: str, password: str, client_id: str):
        """
            Arguments:
                db_host: str    -host-ip or host-name of the mongo database
                db_port: int    -port number of the mongo database
                username: str   -Username of the mongo database
                password: str   -Password for the user account
                client_id: str  -Client ID; Name of the database assigned to the user

            Action:
                Creates a containers object for the user
            
            Return:
                Returns an object of the class Containers
        """
        self.username = username
        self.password = password
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[client_id]
        self.fs = gridfs.GridFS(self.db)
        self.containers = self.db["state"].find_one({"type": "containers"})
        self.container = 'recovery'
    
    def __enter__(self, db_host: str, db_port: int, username: str, password: str, client_id: str):
        """
            Arguments:
                db_host: str    -host-ip or host-name of the mongo database
                db_port: int    -port number of the mongo database
                username: str   -Username of the mongo database
                password: str   -Password for the user account
                client_id: str  -Client ID; Name of the database assigned to the user

            Action:
                Creates a containers object for the user
            
            Return:
                Returns an object of the class Containers
        """
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[client_id]
        self.fs = gridfs.GridFS(self.db)
        self.containers = self.db["state"].find_one({"type": "containers"})
        self.container = 'recovery'

    def list_containers(self) -> list:
        """
            Arguments:

            Action:
                Lists all the containers of the user. All the containers are stored in the db of the name client_id
            
            Return:
                A list of container-names(str)
        """
        container_names = self.containers.copy()
        del container_names['_id']
        del container_names['type']

        return list(container_names.keys())

    def open(self, container_name: str) -> None:
        """
            Arguments:
                container_name: str -The name of the container to be opened
            Action:
                Makes container_name the active container of the container object
            Return:
                None
        """
        self.containers = self.db["state"].find_one({"type": "containers"})
        if container_name not in self.containers:
            self.containers[container_name] = {}

        self.container = container_name

    def list_files(self) -> list:
        """
            Arguments:

            Action:
                Lists all the file names in the active container
            Return:
                Returns a list of all the filenames(str)
        """
        return list(self.containers[self.container].keys())

    def len_files(self) -> int:
        """
            Arguments:

            Action:
                Number of files in the active container
            Return:
                Returns an integer of number of files
        """
        return len(self.containers[self.container].keys())

    def recover_container(self) -> None:
        """
            Arguments:

            Action:
                Recovers missing or un-tracked files and moves them into the recovery container
                A possible reason for missing files could be unexpected program termination
            Return:
                None
        """
        file_ids = set([file._id for file in self.fs.find()])

        del self.containers["_id"], self.containers["type"]
        missing = set()

        for container in self.containers.values():
            missing.update(set(container.values()) - file_ids)

        names = [uuid.uuid1().hex for _ in range(len(missing))]

        self.containers["recovery"].update(zip(names, missing))

        self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                            {"$set": {"recovery": self.containers["recovery"]}},
                                                            return_document=ReturnDocument.AFTER)

    def put(self, files: list, file_names: list = None) -> None:
        """
            Arguments:
                files: list         -A list of file-handlers(read, binary) of binary strings
                file_names: list    -A list of filenames(str). This is an optional argument. Default value is a list of
                                    uuid(s)
            Action:
                Inserts one or more files from the files(list) as filenames(list) in the active container
            Return:
                None
        """
        file_names = file_names or [uuid.uuid1().hex for _ in files]

        container_map = self.containers[self.container]

        try:
            for file, file_name in zip(files, file_names):
                container_map[file_name] = self.fs.put(file, file_name=file_name)
        except Exception as e:
            raise e
        finally:
            self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                                {"$set": {self.container: container_map}},
                                                                return_document=ReturnDocument.AFTER)

    def get(self, file_names: list) -> list:
        """
            Arguments:
                file_names: list    -A list of filenames(syr) to be retrieved
            Action:
                Retrieves a list of file-handlers of the filenames from the file_names(list) in the active container in
                binary read mode
            Return:
                Returns a list of file-handlers
        """
        container_map = self.containers[self.container]

        return [self.fs.get(container_map[file_name], None) for file_name in file_names]

    def delete(self, file_names: list) -> None:
        """
            Arguments:
                file_names: list    -A list of filenames(syr) to be deleted
            Action:
                Deletes all the files of the filenames from the file_names(list) in the active container
            Return:
                None
        """
        container_map = self.containers[self.container]

        try:
            [self.fs.delete(container_map.pop(file_name)) for file_name in file_names if file_name in container_map]
            '''for file_name in file_names:
                if file_name in container_map:
                    self.fs.delete(container_map.pop(file_name))'''
        except Exception as e:
            raise e
        finally:
            self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                                {"$set": {self.container: container_map}},
                                                                return_document=ReturnDocument.AFTER)

    def move(self, destination_container: str, source_file_names: list, destination_file_names: list = None) -> None:
        """
            Arguments:
                destination_container: str      -Destination container name
                source_file_names: list         -A list of filenames(syr) to be moved to the destination container
                destination_file_names: list    -A list of destination filenames(str) to which the moved files will be
                                                renamed to. Default value is the same as source filenames
            Action:
                Moves one or more files of the name in the source_file_names list from the active container to the
                container name of destination_container_name.
                Can be used for renaming files within the same container (destination_container_name is the same as the
                active container name)
            Return:
                None
        """
        destination_file_names = destination_file_names or source_file_names

        source_container_map = self.containers[self.container]
        destination_container_map = self.containers.get(destination_container, {})

        for source_file_name, destination_file_name in zip(source_file_names, destination_file_names):
            if source_file_name in source_container_map:
                destination_container_map[destination_file_name] = source_container_map.pop(source_file_name)

        self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                            {"$set": {
                                                                self.container: source_container_map,
                                                                destination_container: destination_container_map}},
                                                            return_document=ReturnDocument.AFTER)

    def put_one(self, file, file_name: str = uuid.uuid1().hex) -> None:
        """
            Arguments:
                file: file-handler / binary string  -A file handler or the binary string of the data to be put into a
                file in the container
                file_name: str                      -A string file_name. Default value is a uuid
            Action:
                Inserts one file into the active container
            Return:
                None
        """
        container_map = self.containers[self.container]

        container_map[file_name] = self.fs.put(file, file_name=file_name)
        
        self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                            {"$set": {self.container: container_map}},
                                                            return_document=ReturnDocument.AFTER)

    def get_one(self, file_name: str) -> gridfs.grid_file.GridOut:
        """
            Arguments:
                file_name: str  -Name of the file to be retrieved
            Action:
                Get a file handler in binary read mode of the name file_name from the active container
            Return:
                Returns the file handler
        """
        container_map = self.containers[self.container]

        return self.fs.get(container_map[file_name], None)

    def delete_one(self, file_name: str) -> None:
        """
            Arguments:
                file_name: str  -Name of the file to be deleted
            Action:
                Delete a file of the name file_name from the active container
            Return:
                None
        """
        container_map = self.containers[self.container]

        if file_name in container_map:
            self.fs.delete(container_map.pop(file_name))
        
        self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                            {"$set": {self.container: container_map}},
                                                            return_document=ReturnDocument.AFTER)

    def move_one(self, destination_container: str, source_file_name: str, destination_file_name: str = None) -> None:
        """
            Arguments:
                destination_container: str  -Destination container name
                source_file_name: str       -Filename of the file to be moved to the destination container
                destination_file_name: str  -Filename of the file to which the moved file will be renamed to. Default
                value is the same as source filename
            Action:
                Moves one file of the name in the source_file_names list from the active container to the container name
                of destination_container_name.
                Can be used for renaming files within the same container (destination_container_name is the same as the
                active container name)
            Return:
                None
        """
        destination_file_name = destination_file_name or source_file_name

        source_container_map = self.containers[self.container]
        destination_container_map = self.containers.get(destination_container, {})

        destination_container_map[destination_file_name] = source_container_map.pop(source_file_name)

        self.containers = self.db.state.find_one_and_update({"type": "containers"},
                                                            {"$set": {
                                                                self.container: source_container_map,
                                                                destination_container: destination_container_map}},
                                                            return_document=ReturnDocument.AFTER)

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


def main():
    global containers
    containers = Containers("localhost", 27017, "mohit", "password for fs", "80cf72a4083211e9aaacf8cab814c762")


if __name__ == '__main__':
    main()
