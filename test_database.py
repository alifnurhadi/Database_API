import asyncpg
import asyncio
from atribute import DB_alif as db
import psycopg2
from psycopg2.extras import DictCursor


def psycopg2_testing():
    database = db()
    connection_string = database.constring(databasename='alif_db',host=1234)
    
    try:
        # Establish the connection
        conn = psycopg2.connect(connection_string)
        
        # Create a cursor (using DictCursor to get dictionary-like results)
        cur = conn.cursor(cursor_factory=DictCursor)
        
        # Get server version
        cur.execute('SELECT version()')
        server_version = cur.fetchone()[0]
        print(f"[psycopg2] Connected to PostgreSQL server version: {server_version}")
        
        # Fetch data
        cur.execute('SELECT * FROM main;')
        data = cur.fetchall()
        print("[psycopg2] Fetched data:", data)
        
        # Close cursor and connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"[psycopg2] Error: {e}")


async def testing():
    database = db()
    connection_string = database.constring(databasename='alif_db',host=1234)
    
    try:
        # Establish the connection
        conn = await asyncpg.connect(dsn=connection_string)
        
        # Test the connection by fetching the server version
        server_version = await conn.fetchval('SELECT version()')
        print(f"Connected to PostgreSQL server version: {server_version}")
        data = await conn.fetch('SELECT * FROM main;')
        print(data)
        
        # Close the connection
        await conn.close()
        
    except Exception as e:
        print(f"Error: {e}")



# Run both versions
if __name__ == "__main__":
    print("\nTesting psycopg2 (sync):")
    psycopg2_testing()
    print("Testing asyncpg (async):")
    asyncio.run(testing())
    



# import asyncpg
# import psycopg2
# import asyncio
# import time
# import random
# from datetime import datetime, timedelta
# from psycopg2.extras import DictCursor
# from atribute import DB_alif as db

# # Constants for our test
# NUM_RECORDS = 100000  # Number of records to insert
# BATCH_SIZE = 1000    # Size of each batch insert

# async def create_table_async(conn):
#     """Create test table for asyncpg"""
#     await conn.execute('''
#         DROP TABLE IF EXISTS performance_test_async;
#         CREATE TABLE performance_test_async (
#             id SERIAL PRIMARY KEY,
#             name VARCHAR(100),
#             email VARCHAR(100),
#             created_at TIMESTAMP,
#             value DECIMAL(10,2),
#             status BOOLEAN
#         );
#     ''')

# def create_table_sync(conn):
#     """Create test table for psycopg2"""
#     with conn.cursor() as cur:
#         cur.execute('''
#             DROP TABLE IF EXISTS performance_test_sync;
#             CREATE TABLE performance_test_sync (
#                 id SERIAL PRIMARY KEY,
#                 name VARCHAR(100),
#                 email VARCHAR(100),
#                 created_at TIMESTAMP,
#                 value DECIMAL(10,2),
#                 status BOOLEAN
#             );
#         ''')
#     conn.commit()

# def generate_dummy_data(num_records):
#     """Generate dummy data for insertion"""
#     data = []
#     for i in range(num_records):
#         record = (
#             f'User{i}',
#             f'user{i}@example.com',
#             datetime.now() - timedelta(days=random.randint(0, 365)),
#             round(random.uniform(1.0, 1000.0), 2),
#             random.choice([True, False])
#         )
#         data.append(record)
#     return data

# async def insert_async(conn, data):
#     """Insert data using asyncpg"""
#     start_time = time.time()
    
#     # Prepare the statement for batch insert
#     stmt = await conn.prepare('''
#         INSERT INTO performance_test_async (name, email, created_at, value, status)
#         VALUES ($1, $2, $3, $4, $5)
#     ''')
    
#     # Insert in batches
#     for i in range(0, len(data), BATCH_SIZE):
#         batch = data[i:i + BATCH_SIZE]
#         await conn.executemany(
#             'INSERT INTO performance_test_async (name, email, created_at, value, status) VALUES ($1, $2, $3, $4, $5)',
#             batch
#         )
    
#     end_time = time.time()
#     return end_time - start_time

# def insert_sync(conn, data):
#     """Insert data using psycopg2"""
#     start_time = time.time()
    
#     with conn.cursor() as cur:
#         # Insert in batches
#         for i in range(0, len(data), BATCH_SIZE):
#             batch = data[i:i + BATCH_SIZE]
#             psycopg2.extras.execute_batch(cur, '''
#                 INSERT INTO performance_test_sync (name, email, created_at, value, status)
#                 VALUES (%s, %s, %s, %s, %s)
#             ''', batch)
    
#     conn.commit()
#     end_time = time.time()
#     return end_time - start_time

# async def run_async_test():
#     """Run the asyncpg performance test"""
#     database = db()
#     connection_string = database.constring(databasename='alif_db', host=1234)
    
#     print("\nStarting asyncpg test...")
#     conn = await asyncpg.connect(dsn=connection_string)
    
#     # Create table
#     await create_table_async(conn)
    
#     # Generate and insert data
#     data = generate_dummy_data(NUM_RECORDS)
#     insert_time = await insert_async(conn, data)
    
#     # Fetch and time a select query
#     start_time = time.time()
#     count = await conn.fetchval('SELECT COUNT(*) FROM performance_test_async')
#     select_time = time.time() - start_time
    
#     await conn.close()
    
#     return insert_time, select_time, count

# def run_sync_test():
#     """Run the psycopg2 performance test"""
#     database = db()
#     connection_string = database.constring(databasename='alif_db', host=1234)
    
#     print("\nStarting psycopg2 test...")
#     conn = psycopg2.connect(connection_string)
    
#     # Create table
#     create_table_sync(conn)
    
#     # Generate and insert data
#     data = generate_dummy_data(NUM_RECORDS)
#     insert_time = insert_sync(conn, data)
    
#     # Fetch and time a select query
#     start_time = time.time()
#     with conn.cursor() as cur:
#         cur.execute('SELECT COUNT(*) FROM performance_test_sync')
#         count = cur.fetchone()[0]
#     select_time = time.time() - start_time
    
#     conn.close()
    
#     return insert_time, select_time, count

# async def main():
#     # Run async test
#     async_insert_time, async_select_time, async_count = await run_async_test()
    
#     # Run sync test
#     sync_insert_time, sync_select_time, sync_count = run_sync_test()
    
#     # Print results
#     print("\nPerformance Test Results:")
#     print(f"Number of records: {NUM_RECORDS}")
#     print(f"Batch size: {BATCH_SIZE}")
#     print("\nAsyncpg Results:")
#     print(f"Insert time: {async_insert_time:.2f} seconds")
#     print(f"Select time: {async_select_time:.2f} seconds")
#     print(f"Record count: {async_count}")
#     print("\nPsycopg2 Results:")
#     print(f"Insert time: {sync_insert_time:.2f} seconds")
#     print(f"Select time: {sync_select_time:.2f} seconds")
#     print(f"Record count: {sync_count}")

# if __name__ == "__main__":
#     asyncio.run(main())

'''
RESULT 


Starting asyncpg test...

Starting psycopg2 test...

Performance Test Results:
Number of records: 100000
Batch size: 1000

Asyncpg Results:
Insert time: 2.45 seconds
Select time: 0.01 seconds
Record count: 100000

Psycopg2 Results:
Insert time: 54.74 seconds
Select time: 0.00 seconds
Record count: 100000
'''

# import asyncpg
# import psycopg2
# import asyncio
# import time
# from datetime import datetime, timedelta
# from psycopg2.extras import DictCursor
# from atribute import DB_alif as db
# import statistics

# class ReadPerformanceTest:
#     def __init__(self, connection_string: str):
#         self.connection_string = connection_string
#         self.num_iterations = 5  # Number of times to run each test
        
#     async def run_async_queries(self):
#         """Run read tests using asyncpg"""
#         conn = await asyncpg.connect(dsn=self.connection_string)
#         results = {}
        
#         try:
#             # Test 1: Simple SELECT
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await conn.fetch('SELECT * FROM performance_test_async')
#                 times.append(time.perf_counter() - start)
#             results['simple_select'] = times

#             # Test 2: COUNT
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await conn.fetchval('SELECT COUNT(*) FROM performance_test_async')
#                 times.append(time.perf_counter() - start)
#             results['count'] = times

#             # Test 3: GROUP BY
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await conn.fetch('SELECT email, COUNT(*) FROM performance_test_async GROUP BY email')
#                 times.append(time.perf_counter() - start)
#             results['group_by'] = times

#             # Test 4: ORDER BY with LIMIT
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await conn.fetch('SELECT * FROM performance_test_async ORDER BY email LIMIT 100')
#                 times.append(time.perf_counter() - start)
#             results['order_by_limit'] = times

#         finally:
#             await conn.close()
            
#         return results

#     def run_sync_queries(self):
#         """Run read tests using psycopg2"""
#         results = {}
        
#         with psycopg2.connect(self.connection_string) as conn:
#             with conn.cursor() as cur:
#                 # Test 1: Simple SELECT
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('SELECT * FROM performance_test_sync')
#                     cur.fetchall()
#                     times.append(time.perf_counter() - start)
#                 results['simple_select'] = times

#                 # Test 2: COUNT
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('SELECT COUNT(*) FROM performance_test_sync')
#                     cur.fetchone()
#                     times.append(time.perf_counter() - start)
#                 results['count'] = times

#                 # Test 3: GROUP BY
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('SELECT email, COUNT(*) FROM performance_test_sync GROUP BY email')
#                     cur.fetchall()
#                     times.append(time.perf_counter() - start)
#                 results['group_by'] = times

#                 # Test 4: ORDER BY with LIMIT
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('SELECT * FROM performance_test_sync ORDER BY email LIMIT 100')
#                     cur.fetchall()
#                     times.append(time.perf_counter() - start)
#                 results['order_by_limit'] = times
                
#         return results

# async def main():
#     # Initialize database connection
#     database = db()
#     connection_string = database.constring(databasename='alif_db', host=1234)
    
#     # Create test instance
#     test = ReadPerformanceTest(connection_string)
    
#     print("\nStarting Read Performance Tests...")
#     print(f"Running {test.num_iterations} iterations for each test")
#     print("\nRunning asyncpg tests...")
#     async_results = await test.run_async_queries()
    
#     print("Running psycopg2 tests...")
#     sync_results = test.run_sync_queries()
    
#     # Print results in a formatted table
#     print("\nPerformance Results (times in milliseconds):")
#     print("-" * 80)
#     print(f"{'Query Type':<20} {'Asyncpg (avg)':<15} {'Psycopg2 (avg)':<15} {'Speedup Factor':<15}")
#     print("-" * 80)
    
#     for query_type in async_results.keys():
#         async_avg = statistics.mean(async_results[query_type]) * 1000  # Convert to ms
#         sync_avg = statistics.mean(sync_results[query_type]) * 1000    # Convert to ms
#         speedup = sync_avg / async_avg if async_avg > 0 else 0
        
#         print(f"{query_type:<20} {async_avg:>14.2f} {sync_avg:>14.2f} {speedup:>14.2f}x")
    
#     print("-" * 80)
    
#     # Print detailed statistics
#     print("\nDetailed Statistics:")
#     for query_type in async_results.keys():
#         print(f"\n{query_type.replace('_', ' ').title()}:")
        
#         async_times = async_results[query_type]
#         sync_times = sync_results[query_type]
        
#         print("  Asyncpg:")
#         print(f"    Min: {min(async_times)*1000:.2f}ms")
#         print(f"    Max: {max(async_times)*1000:.2f}ms")
#         print(f"    Std Dev: {statistics.stdev(async_times)*1000:.2f}ms")
        
#         print("  Psycopg2:")
#         print(f"    Min: {min(sync_times)*1000:.2f}ms")
#         print(f"    Max: {max(sync_times)*1000:.2f}ms")
#         print(f"    Std Dev: {statistics.stdev(sync_times)*1000:.2f}ms")

# if __name__ == "__main__":
#     asyncio.run(main())

'''
Starting Read Performance Tests...
Running 5 iterations for each test

Running asyncpg tests...
Running psycopg2 tests...

Performance Results (times in milliseconds):
--------------------------------------------------------------------------------
Query Type           Asyncpg (avg)   Psycopg2 (avg)  Speedup Factor
--------------------------------------------------------------------------------
simple_select                231.69         181.93           0.79x
count                          8.56           5.68           0.66x
group_by                     252.21         284.00           1.13x
order_by_limit                58.27          52.83           0.91x
--------------------------------------------------------------------------------

Detailed Statistics:

Simple Select:
  Asyncpg:
    Min: 210.95ms
    Max: 248.38ms
    Std Dev: 13.80ms
  Psycopg2:
    Min: 177.59ms
    Max: 185.41ms
    Std Dev: 2.94ms

Count:
  Asyncpg:
    Min: 5.32ms
    Max: 20.50ms
    Std Dev: 6.67ms
  Psycopg2:
    Min: 4.72ms
    Max: 8.91ms
    Std Dev: 1.81ms

Group By:
  Asyncpg:
    Min: 244.42ms
    Max: 277.28ms
    Std Dev: 14.09ms
  Psycopg2:
    Min: 245.39ms
    Max: 349.72ms
    Std Dev: 40.49ms

Order By Limit:
  Asyncpg:
    Min: 52.05ms
    Max: 79.40ms
    Std Dev: 11.84ms
  Psycopg2:
    Min: 50.14ms
    Max: 55.59ms
    Std Dev: 2.40ms
'''

# import asyncpg
# import psycopg2
# import asyncio
# import time
# from contextlib import contextmanager
# from psycopg2.pool import SimpleConnectionPool
# from atribute import DB_alif as db
# import statistics

# class OptimizedReadPerformanceTest:
#     def __init__(self, connection_string: str):
#         self.connection_string = connection_string
#         self.num_iterations = 5
#         self.pool_size = 5
        
#         # Create connection pools
#         self.pg_pool = SimpleConnectionPool(1, self.pool_size, connection_string)
    
#     async def init_async_pool(self):
#         self.async_pool = await asyncpg.create_pool(
#             dsn=self.connection_string,
#             min_size=1,
#             max_size=self.pool_size,
#             command_timeout=60
#         )

#     @contextmanager
#     def get_sync_connection(self):
#         conn = self.pg_pool.getconn()
#         try:
#             yield conn
#         finally:
#             self.pg_pool.putconn(conn)

#     async def run_pooled_async_queries(self):
#         """Run read tests using asyncpg with connection pool"""
#         results = {}
        
#         async with self.async_pool.acquire() as conn:
#             # Prepare statements
#             simple_stmt = await conn.prepare('SELECT * FROM performance_test_async')
#             count_stmt = await conn.prepare('SELECT COUNT(*) FROM performance_test_async')
#             group_stmt = await conn.prepare('SELECT email, COUNT(*) FROM performance_test_async GROUP BY email')
#             order_stmt = await conn.prepare('SELECT * FROM performance_test_async ORDER BY email LIMIT 100')
            
#             # Test 1: Simple SELECT with prepared statement
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await simple_stmt.fetch()
#                 times.append(time.perf_counter() - start)
#             results['simple_select'] = times

#             # Test 2: COUNT with prepared statement
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await count_stmt.fetchval()
#                 times.append(time.perf_counter() - start)
#             results['count'] = times

#             # Test 3: GROUP BY with prepared statement
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await group_stmt.fetch()
#                 times.append(time.perf_counter() - start)
#             results['group_by'] = times

#             # Test 4: ORDER BY with prepared statement
#             times = []
#             for _ in range(self.num_iterations):
#                 start = time.perf_counter()
#                 await order_stmt.fetch()
#                 times.append(time.perf_counter() - start)
#             results['order_by_limit'] = times

#         return results

#     def run_pooled_sync_queries(self):
#         """Run read tests using psycopg2 with connection pool"""
#         results = {}
        
#         with self.get_sync_connection() as conn:
#             with conn.cursor() as cur:
#                 # Prepare statements
#                 cur.execute('PREPARE simple_select AS SELECT * FROM performance_test_sync')
#                 cur.execute('PREPARE count_select AS SELECT COUNT(*) FROM performance_test_sync')
#                 cur.execute('PREPARE group_select AS SELECT email, COUNT(*) FROM performance_test_sync GROUP BY email')
#                 cur.execute('PREPARE order_select AS SELECT * FROM performance_test_sync ORDER BY email LIMIT 100')
                
#                 # Test 1: Simple SELECT with prepared statement
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('EXECUTE simple_select')
#                     cur.fetchall()
#                     times.append(time.perf_counter() - start)
#                 results['simple_select'] = times

#                 # Test 2: COUNT with prepared statement
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('EXECUTE count_select')
#                     cur.fetchone()
#                     times.append(time.perf_counter() - start)
#                 results['count'] = times

#                 # Test 3: GROUP BY with prepared statement
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('EXECUTE group_select')
#                     cur.fetchall()
#                     times.append(time.perf_counter() - start)
#                 results['group_by'] = times

#                 # Test 4: ORDER BY with prepared statement
#                 times = []
#                 for _ in range(self.num_iterations):
#                     start = time.perf_counter()
#                     cur.execute('EXECUTE order_select')
#                     cur.fetchall()
#                     times.append(time.perf_counter() - start)
#                 results['order_by_limit'] = times
                
#         return results

# async def main():
#     database = db()
#     connection_string = database.constring(databasename='alif_db', host=1234)
    
#     test = OptimizedReadPerformanceTest(connection_string)
#     await test.init_async_pool()
    
#     print("\nStarting Optimized Read Performance Tests...")
#     print(f"Running {test.num_iterations} iterations for each test")
#     print(f"Using connection pool with {test.pool_size} connections")
    
#     print("\nRunning asyncpg tests with connection pool...")
#     async_results = await test.run_pooled_async_queries()
    
#     print("Running psycopg2 tests with connection pool...")
#     sync_results = test.run_pooled_sync_queries()
    
#     # Print results
#     print("\nPerformance Results (times in milliseconds):")
#     print("-" * 80)
#     print(f"{'Query Type':<20} {'Asyncpg (avg)':<15} {'Psycopg2 (avg)':<15} {'Ratio':<15}")
#     print("-" * 80)
    
#     for query_type in async_results.keys():
#         async_avg = statistics.mean(async_results[query_type]) * 1000
#         sync_avg = statistics.mean(sync_results[query_type]) * 1000
#         ratio = sync_avg / async_avg if async_avg > 0 else 0
        
#         print(f"{query_type:<20} {async_avg:>14.2f} {sync_avg:>14.2f} {ratio:>14.2f}x")
    
#     # Cleanup
#     test.pg_pool.closeall()
#     await test.async_pool.close()

# if __name__ == "__main__":
#     asyncio.run(main())


'''
Starting Optimized Read Performance Tests...
Running 5 iterations for each test
Using connection pool with 5 connections

Running asyncpg tests with connection pool...
Running psycopg2 tests with connection pool...

Performance Results (times in milliseconds):
--------------------------------------------------------------------------------
Query Type           Asyncpg (avg)   Psycopg2 (avg)  Ratio
--------------------------------------------------------------------------------
simple_select                230.96         430.38           1.86x
count                          6.42           5.61           0.87x
group_by                     220.82         405.79           1.84x
order_by_limit                60.21          52.23           0.87x

GOOD WHEN WE HAVE SOME KINDA MULTIPLE READ
'''