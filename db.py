from datetime import datetime
from fastapi import FastAPI,  HTTPException
import polars as pl
from pydantic import BaseModel , Field
from typing import Any, AsyncGenerator, Dict, Generator, List, Optional
from contextlib import asynccontextmanager
import asyncpg
from atribute import DB_alif


class BaseResponse(BaseModel):
    status: str = Field(default="success")
    timestamp: datetime = Field(default_factory=datetime.timestamp)

class DataResponse(BaseResponse):
    '''
        {
    "status": "success",
    "timestamp": datetime,  # Timestamp will vary
    "data": [
        {"id": 1, "name": "Item 1"},
        {"id": 2, "name": "Item 2"}
    ],
    "total_records": 2
    }
    '''
    data: List[Dict[str, Any]] = Field(default_factory=list)
    total_records: int = Field(default=0)
    
class MessageResponse(BaseResponse):
    '''{
        "status": "success",
        "timestamp": datetime,  # Timestamp will vary
        "message": " some message "
        }
    '''
    message: str



class Database:
    def __init__(self, version :str = None):

        if not version:
            raise ValueError ('it need to define what database are going to used')
        
        if not isinstance(version, str):
            raise TypeError (' put some string format connection for this job')
        
        if version in {'pg1','pg2','pg3'}:
            db = DB_alif()
            if version == 'pg1':
                self.db = db.constring(databasename='alif_db' , host='1234')
            elif version == 'pg2':
                self.db = db.constring(databasename='alif_db' , host='1234')
            elif version == 'pg3':
                self.db = db.constring(databasename='alif_db' , host='1234')
        else :
            raise ValueError(f"Unsupported database version: {version}")

    @asynccontextmanager
    async def start_pool(self)-> Generator[asyncpg.Pool , None , None ]:
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(dsn=self.db , min_size=2 , max_size=5)
                return self.pool
            except HTTPException:
                raise HTTPException(status_code=505 , detail='can"t create a pool connection, try another url')

    async def close(self):
        if self.pool:
            await self.pool.release()
            self.pool = None

    @asynccontextmanager
    async def connect(self):
        async with self.start_pool() as pools:
            async with pools.acquire() as conn:
                yield conn
    
    async def fetch(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        async with self.connect() as conn:
            try:
                rows = await conn.fetch(query, *params) if params else await conn.fetch(query)
                return [dict(row) for row in rows]
            except asyncpg.PostgresError as e:
                raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")
                    
    async def execute(self, query: str ,params: tuple = None) -> str:
        async with self.connect() as conn:
            try:
                result = await conn.execute(query, *params) if params else await conn.execute(query)
                return result
            except asyncpg.PostgresError as e:
                raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


class ParquetData:
    def __init__(self , newpath:str=None , anotherpath:str = None , partition_col:list[str,str]=None) -> None:
        self.existpath = './data/sales.parquet'
        self.newpath = newpath
        self.another = anotherpath
        self.partition = partition_col
        self._sql_env = None

    @property
    async def sql_env(self)->pl.SQLContext:
        if self._sql_env is None:
            data = pl.scan_parquet(self.existpath)
            self._sql_env = pl.SQLContext()
            self._sql_env.register('df', data)
        return self._sql_env

    async def read(self , query:str):
        sql_context = await self.sql_env
        try:
            query = query or 'select * from df'
            # Chain methods after awaiting the execute
            result: pl.DataFrame = await sql_context.execute(query)

            return result.to_dicts()
        except:
            raise HTTPException(status_code=500,detail=f"Failed to execute query"  )

    async def write(self,newdata:dict, sendto_newpath:bool=False , sendto_another:bool=False)->None:
        data = pl.scan_parquet(self.existpath)
        new = pl.LazyFrame(newdata)
        
        try:
            concat = pl.concat([data,new]).collect(streaming=True)
            output_path = (
                self.newpath if sendto_newpath
                else self.another if sendto_another
                else self.existpath
            )
            
            concat.write_parquet(
                output_path,
                compression="zstd",
                partition_by=self.partition
            )
        except Exception as e :
            raise Exception(f'Failed to load new data, there"s an issue on {e}')


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize services
    app.state.db = Database('pg1')
    app.state.db2 = Database('pg2')
    app.state.pqt = ParquetData(
        newpath='./data/marketing.parquet',
        anotherpath='/data/summary.parquet'
    )
    # Starting-up
    await app.state.db.start_pool()
    yield
    # Shuting-down
    await app.state.db.close()

app = FastAPI(lifespan=lifespan)

@app.get('/get',response_model= DataResponse)
async def fetch_db(queries:str,params: tuple = None):
    try:
        data = await app.state.db.fetch(queries,params)
        if not data:
            raise ValueError('put some connection string value')
        return DataResponse(data=data, total_records=len(data))
    except:
        raise HTTPException(status_code=505 ,detail="can't fetch data from the databases, make sure the query are having the appropiate syntax")

@app.get('/pqt',response_model= DataResponse)
async def get_data(queries :str = None):
    try:
        if queries:
            data = await app.state.pqt.read(queries)

        return DataResponse(
            data=data , total_records=len(data)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/push')
async def insert_db(query: str, params: tuple = None):
    try:
        result = await app.state.db.execute(query , params)
        return MessageResponse(message=f"Query executed successfully: {result}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/pqt')
async def push_data(some_data: dict):
    try:
        await app.state.pqt.write(some_data)
        return MessageResponse(message="Data added successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete('/remove')
async def rm_db(query: str):
    try:
        result = await app.state.db.execute(query)
        return MessageResponse(message=f"Delete executed successfully: {result}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

