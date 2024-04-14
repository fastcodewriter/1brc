import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from typing import Dict, List
import mmap


def process_chunk(file_name: str, start: int, end: int) -> Dict[bytes, List[float]]:
    """Process a chunk of the file and return the temperature data."""
    
    temperature_data = defaultdict(lambda: [float('inf'), 0, float('-inf'), 0])
    
    with open(file_name, 'rb') as file:
        
        mmapped_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        
        if start > 0:
            mmapped_file.seek(start - 1)
            if mmapped_file.read(1) != '\n':
                mmapped_file.readline()
        else:
            mmapped_file.seek(start)

        while mmapped_file.tell() < end:
            station, temperature = mmapped_file.readline().split(b';')
            temperature = float(temperature) 

            _data = temperature_data[station]

            if _data[0] > temperature:
                _data[0] = temperature
            if _data[2] < temperature:
                _data[2] = temperature

            _data[1] += temperature
            _data[3] += 1

    return dict(temperature_data)


def process_file(file_name: str, num_processors: int) -> Dict[bytes, List[float]]:
    """Process the file and return the temperature data."""
    
    all_results = defaultdict(lambda: [float('inf'), 0, float('-inf'), 0])
    
    with ProcessPoolExecutor(max_workers=num_processors) as executor:
        
        file_size = os.path.getsize(file_name)
        chunk_size = file_size // num_processors

        futures = {
            executor.submit(
                process_chunk,
                file_name, 
                chunk_size * i,
                chunk_size * (i + 1)
                ): i for i in range(num_processors)
            }
        
        for future in as_completed(futures):
            
            result = future.result()
            
            for station, temps in result.items():
                _data = all_results[station]
                if _data[0] > temps[0]:
                    _data[0] = temps[0]
                if _data[2] < temps[2]:
                    _data[2] = temps[2]

                _data[1] += temps[1]
                _data[3] += temps[3]
    
    return all_results

def print_results(results: Dict[bytes, List[float]]):
    """Print the results in the required format."""
    print("{", end="")
    for location, measurements in sorted(results.items()):
        print(
            f'{location.decode('UTF8')}='
            f'{measurements[0]}/'
            f'{(measurements[1]) / measurements[3]:.1f}/'
            f'{(measurements[2])}',
            end=", ",
        )
    print("\b\b} ")


if __name__ == '__main__':
    num_processors = 8 
    file_name = 'measurements.txt'
    results = process_file(file_name, num_processors)

    print_results(results)
