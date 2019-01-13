from pymongo import MongoClient, ReturnDocument
import gridfs, uuid

containers = None

class Containers:
    def __init__(self, db_host: str, db_port: int, username: str, password: str, ID: str):
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[ID]
        self.fs = gridfs.GridFS(self.db)
        self.containers = self.db["state"].find_one({"type": "containers"})

    def list_containers(self):
        return list(self.containers.keys())

    def open(self, container_name: str) -> None:
        if container_name not in self.containers:
            self.containers[container_name] = {}

        self.container = container_name

    def list_files(self):
        return list(self.containers[self.container].keys())

    def len_files(self):
        return len(self.containers[self.container].keys())

    def recover_container(self):
        file_ids = set([file._id for file in self.fs.find()])

        del self.containers["_id"], self.containers["type"]
        missing = set()

        for container in self.containers.values():
            missing.update(set(container.values()) - file_ids)

        names = [uuid.uuid1().hex for _ in range(len(missing))]

        self.containers["recovery"].update(zip(names, missing))

        self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {"recovery": self.containers["recovery"]}}, 
                return_document=ReturnDocument.AFTER)

    def put(self, files, file_names: list = None) -> None:
        file_names = file_names or [uuid.uuid1().hex for _ in files]

        container_map = self.containers[self.container]

        try:
            for file, file_name in zip(files, file_names):
                if file_name not in container_map:
                    container_map[file_name] = self.fs.put(file, file_name=file_name)
        except Exception as e:
            raise e
        finally:
            self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {self.container: container_map}}, 
                return_document=ReturnDocument.AFTER)

    def get(self, file_names: list) -> list:
        container_map = self.containers[self.container]

        return [self.fs.get(container_map[file_name]) for file_name in file_names]

    def delete(self, file_names: list) -> None:
        container_map = self.containers[self.container]

        try:
            for file_name in file_names:
                if file_name in container_map:
                    self.fs.delete(container_map[file_name])
                    del container_map[file_name]
        except Exception as e:
            raise e
        finally:
            self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {self.container: container_map}}, 
                return_document=ReturnDocument.AFTER)

    def move(self, destination_container, source_file_names, destination_file_names = None):
        destination_file_names = destination_file_names or source_file_names

        source_container_map = self.containers[self.container]
        destination_container_map = self.containers.get(destination_container, {})

        for source_file_name, destination_file_name in zip(source_file_names, destination_file_names):
            if source_file_name in source_container_map:
                destination_container_map[destination_file_name] = source_container_map[source_file_name]
                del source_container_map[source_file_name]

        self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {self.container: source_container_map, 
                destination_container: destination_container_map}}, return_document=ReturnDocument.AFTER)

    def put_one(self, file, file_name: str = uuid.uuid1().hex) -> None:
        container_map = self.containers[self.container]

        if file_name not in container_map:
            container_map[file_name] = self.fs.put(file, file_name=file_name)
        
        self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {self.container: container_map}}, 
                return_document=ReturnDocument.AFTER)

    def get_one(self, file_name: str):
        container_map = self.containers[self.container]

        return self.fs.get(container_map[file_name])

    def delete_one(self, file_name: str) -> None:
        container_map = self.containers[self.container]

        if file_name in container_map:
            self.fs.delete(container_map[file_name])
            del container_map[file_name]
        
        self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {self.container: container_map}}, 
                return_document=ReturnDocument.AFTER)

    def move_one(self, destination_container, source_file_name, destination_file_name = None):
        destination_file_name = destination_file_name or source_file_name

        source_container_map = self.containers[self.container]
        destination_container_map = self.containers.get(destination_container, {})

        destination_container_map[destination_file_name] = source_container_map[source_file_name]
        del source_container_map[source_file_name]

        self.containers = self.db.state.find_one_and_update({"type": "containers"}, {"$set": {self.container: source_container_map, 
                destination_container: destination_container_map}}, return_document=ReturnDocument.AFTER)



def main():
    global containers
    containers = Containers("localhost", 27017, "mohit", "password for fs", "mohit")

    print(containers.list_containers())


main()
