
pg_url = ...
class DB_alif:
    def __init__(self) -> None:
        self.user = 'alifnhd'
        self.passw = 'nIs_RSKLw6NBlMn5OL1t'

    def username(self):
        POSTGRES_USER = 'alifnhd'
        return POSTGRES_USER
    
    def password(self):
        POSTGRES_PASSWORD = 'nIs_RSKLw6NBlMn5OL1t'
        return POSTGRES_PASSWORD

    def constring(self,databasename:str , host):
        return f'postgresql://{self.user}:{self.passw}@localhost:{host}/{databasename}'
