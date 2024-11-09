from fastapi import FastAPI
import polars as pl
from pydantic import BaseModel
from typing import AsyncGenerator, Generator, Optional
import asyncpg
from atribute import DB_alif

class User(BaseModel):
    name : str
    email : str
    identity : int
    password : any

class Responses(BaseModel):
    table : dict   


class Database:
    async def __init__(self, version :str = None):

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

class Connect(Database):
    def __init__(self, version: str = 'pg1'):
        super().__init__(version)
        self.pool = None # additional i dont will work or not 

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(dsn=self.db)

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    # OG
    async def fetch_some(self,query):
        async with self.connect() as pool:
            async with pool.acquire() as con:
                return await con.fetchrow(query)
            
    #opsi
    async def fetch_some(self, query: str) -> AsyncGenerator[dict, None]:
        await self.connect()  # Ensures the pool is initialized
        async with self.pool.acquire() as con:
            # Fetch multiple rows to simulate a generator-like output
            rows = await con.fetch(query)
            for row in rows:
                yield dict(row)
    
    # OG
    async def execute_query(self, query: str, params: Optional[dict] = None):
        async with self.connect() as pool:
            async with pool.acquire() as con:
                return await con.execute(query, *params)
            
    # opsi
    async def execute_query(self, query: str, params: Optional[tuple] = None) -> str:
        await self.connect()  # Ensures the pool is initialized
        async with self.pool.acquire() as con:
            # Execute the query, params must be a tuple for *params
            return await con.execute(query, *params) if params else await con.execute(query)


app = FastAPI()


@app.get('/get')
async def fetch_db(Query:str):
    return await Database.connect().fetchrow(Query)

@app.get('/pqt')
async def get_data(some_condition:str|int = None):
    data:pl.DataFrame = pl.read_parquet('someparquet file')
    data:pl.DataFrame = data.filter(some_condition)
    return await data.to_dicts() ## i hope this line is similar when we use to dict and orient format.

@app.post('/push')
async def insert_db(query:str):
    return await Database.connect().executemany(query)

@app.post('p.pqt')
async def push_data(some_data:dict):
    old:pl.LazyFrame = pl.scan_parquet('someparquet')
    new:pl.LazyFrame = pl.LazyFrame(some_data)
    combine : pl.LazyFrame = pl.concat([old,new])
    return await combine.sink_parquet('someparquet')


@app.delete('/remove')
async def rm_db(query:str):
    return await query

@app.delete('rm.pqt')
async def remove(some_condition):
    data : pl.LazyFrame = pl.scan_parquet('someparquet')
    data = data.drop(some_condition)
    return await data.sink_parquet('someparquet')


'''

options 

@app.get('/get')
async def fetch_db(query: str):
    try:
        data = await Connect().fetch_some(query)
        if not data:
            raise HTTPException(status_code=404, detail="Data not found")
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/pqt')
async def get_data(some_condition: Optional[str] = None):
    try:
        data: pl.DataFrame = pl.read_parquet('someparquet')
        if some_condition:
            data = data.filter(pl.col(some_condition).is_not_null())
        return data.to_dicts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/push')
async def insert_db(query: str):
    try:
        result = await Connect().execute_query(query)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/p.pqt')
async def push_data(some_data: dict):
    try:
        old: pl.LazyFrame = pl.scan_parquet('someparquet')
        new: pl.LazyFrame = pl.LazyFrame(some_data)
        combined: pl.LazyFrame = pl.concat([old, new])
        combined.write_parquet('someparquet')
        return {"status": "success", "message": "Data added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete('/remove')
async def rm_db(query: str):
    try:
        result = await Connect().execute_query(query)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete('/rm.pqt')
async def remove(some_condition: str):
    try:
        data: pl.LazyFrame = pl.scan_parquet('someparquet')
        data = data.filter(pl.col(some_condition).is_not_null())  # Assuming it's a condition to drop rows
        data.write_parquet('someparquet')
        return {"status": "success", "message": "Data removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


'''