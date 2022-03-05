"""
Implementar un merge concurrente:
- Tenemos NPROD procesos que producen números no negativos de forma
creciente. Cuando un proceso acaba de producir, produce un -1

- Hay un proceso merge que debe tomar los números y almacenarlos de
forma creciente en una única lista (o array). El proceso debe esperar a que
los productores tengan listo un elemento e introducir el menor de
ellos.

- Se debe crear listas de semáforos. Cada productor solo maneja los
sus semáforos para sus datos. El proceso merge debe manejar todos los
semáforos.

- OPCIONAL: se puede hacer un búffer de tamaño fijo de forma que
los productores ponen valores en el búffer.
"""

"""
VERSIÓN I: Se dispone de un buffer que almacena un único elemento producido por 
cada productor que no ha sido aún consumido.
"""
# Importamos los paquetes necesarios
from multiprocessing import Process, Manager, BoundedSemaphore, Semaphore, Lock
from multiprocessing import Array
from random import randint

# Fijamos el número de productores
NPROD = 3

"""
La función add_data añade a la posición correspondiente del buffer el dato 
producido por el productor pid. Se protege con un Lock (mutex) para que 
productor y consumidor no puedan producir y consumir a la vez.
"""
def add_data(buffer, pid, dato, mutex):
    mutex.acquire()
    try:
        buffer[pid] = dato
    finally:
        mutex.release()

"""
La función get_data obtiene el elemento mínimo del buffer y la posición en la 
que se encuentra. Se protege con un Lock (mutex) para que productor y 
consumidor no puedan producir y consumir a la vez.
"""
def get_data(buffer, mutex):
    mutex.acquire()
    try:
        minimum = max(buffer)        
        posmin = list(buffer).index(minimum)
        for i in range(NPROD):
            if buffer[i] == -1:
                pass
            else:
                if buffer[i] < minimum:
                    minimum = buffer[i]
                    posmin = i
    finally:
        mutex.release()
    return minimum, posmin

"""
La función producer corresponde a la tarea de producir una cantidad dada de 
números almacenándolos en la posición correspondiente del buffer
"""
def producer(pid, nonEmpty, nonFull, buffer, mutex, cantidad):
    v = 0
    for dato in range(cantidad):
        v += randint(1,6)
        nonFull.acquire() # wait nonFull
        # Se añade el dato generado al buffer
        add_data(buffer, pid, v, mutex)
        print (f"Productor {pid} almacenando {v}")
        nonEmpty.release() # signal nonEmpty
    # El productor produce un -1 para indicar que ha terminado de producir
    nonFull.acquire() # wait nonFull
    add_data(buffer, pid, -1, mutex) # Añadimos el -1 al buffer
    nonEmpty.release() # wait nonEmpty

"""
La función consumer corresponde a la acción de consumir el menor elemento del
buffer.
"""
def consumer(lsemaphores, buffer, result, mutex):
    # Primeramente, nos aseguramos de que todos los productores hayan producido
    for pid in range(NPROD):
        if buffer[pid] != -1:
            lsemaphores[2*pid].acquire() # wait nonEmpty pid
    while list(buffer) != [-1]*NPROD: # hasta que los NPROD productores dejen de producir
        # Seleccionamos el menor dato del buffer y el productor correspondiente
        dato, pid = get_data(buffer, mutex)
        print (f"Consumiendo {dato} del productor {pid}")
        # Lo añadimos a la lista resultado
        result.append(dato)
        print(f"result = {result}")
        lsemaphores[2*pid + 1].release() # signal nonFull pid
        lsemaphores[2*pid].acquire() # wait nonEmpty pid
        print("Buffer = " +str(list(buffer)))

def main():
    manager = Manager()
    result = manager.list() # Lista solución de números ordenados
    
    lsemaphores = []
    for i in range(NPROD):
        lsemaphores.append(Semaphore (0)) # semáforo non-Empty 
        lsemaphores.append(BoundedSemaphore (1)) # semáforo non-Full 
        
    buffer = Array('i', NPROD)
    
    mutex = Lock()
    
    prodlst = [Process(target=producer, name=f'Productor {pid}', 
                       args=(pid, lsemaphores[2*pid], lsemaphores[2*pid + 1], buffer,
                             mutex, randint(1,20))) for pid in range(NPROD) ]
    merge = Process (target = consumer, name ='Consumidor', 
                     args = (lsemaphores, buffer, result, mutex))
    
    for p in prodlst + [merge]:
        p.start()

    for p in prodlst + [merge]:
        p.join()
        
    print(f"result = {result}")

if __name__ == '__main__':
    main()

                