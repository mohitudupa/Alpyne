# Alpyne

## A simple, easy install framework for grid computing

## Prerequisites
    1:  Install python 3.7 or higher and pip (if needed)
>
    2:  Install pipenv
```
Code:
    $ pip install pipenv
```
>
    3:  Setup a mongo-db server on local host at default port
        and setup the required user collections

---

## Installation instructions

    1:  Clone the repository into a project directory
>
    2:  run python setup.py
        This will setup the .config and .env files for the project
        in the project root directory
    
```
Code:
    $ python setup.py
```
    3:  Sync and download the dependencies for the project
```
Code:
    $ pipenv sync
```
    4:  Start the virtual environment
```
Code:
    $ pipenv shell
```
    5:  Run the server from either the project directory or the server
        (./Compute) root directiry
```
Code:
    $ python Compute/manage.py runserver

OR
    $ cd Compute
    $ python manage.py runserver
```

---
