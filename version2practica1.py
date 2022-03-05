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
VERSIÓN II: Se dispone de un buffer que almacena a lo sumo K elementos producidos
por cada productor que aún no han sido consumidos
"""
# Importamos los paquetes necesarios
from multiprocessing import Process, Manager, BoundedSemaphore, Semaphore, Lock
from multiprocessing import Array
from random import randint

K = 10 # capacidad máxima de almacenamiento en el buffer de cada productor
NPROD = 3 #número de productores

"""
La función add_data añade a la posición correspondiente del buffer el dato 
producido por el productor pid. Se protege con un Lock (mutex) para que 
productor y consumidor no puedan producir y consumir a la vez.
"""        
def add_data(buffer, ultimos, pid, dato, mutex):
    mutex.acquire()
    try:
        ultimos[pid] = ultimos[pid] + 1
        buffer[pid*K + ultimos[pid]] = dato
    finally:
        mutex.release()

"""
La función get_data obtiene el elemento mínimo del buffer y el productor que
lo produjo. Se protege con un Lock (mutex) para que productor y consumidor 
no puedan producir y consumir a la vez.
"""
def get_data(buffer, mutex):
    mutex.acquire()
    try:
        primeros = [buffer[K*i] for i in range(NPROD)]
        minimum = max(primeros)        
        posmin = list(primeros).index(minimum)
        for i in range(NPROD):
            if primeros[i] == -1:
                pass
            else:
                if primeros[i] < minimum:
                    minimum = primeros[i]
                    posmin = i
    finally:
        mutex.release()
    return minimum, posmin

"""
La función producer corresponde a la tarea de producir una cantidad dada de 
números almacenándolos en la posición correspondiente del buffer
"""
def producer(pid, nonEmpty, nonFull, buffer, ultimos, mutex, numero):
    v = 0
    for dato in range(numero):
        v += randint(1,6)
        nonFull.acquire() # wait nonFull
        # Se añade el dato generado al buffer
        add_data(buffer, ultimos, pid, v, mutex)
        print (f"Productor {pid} almacenando {v}")
        nonEmpty.release() # signal nonEmpty
    # El productor produce un -1 para indicar que ha terminado de producir
    nonFull.acquire() # wait nonFull
    add_data(buffer, ultimos, pid, -1, mutex) # Añadimos el -1 al buffer
    nonEmpty.release() # wait nonEmpty

"""
La función consumer corresponde a la acción de consumir el menor elemento del
buffer.
"""
def consumer(lsemaphores, buffer, ultimos, result, mutex):
    # Primeramente, nos aseguramos de que todos los productores hayan producido
    for pid in range(NPROD):
        if ultimos[pid] == -1:
            lsemaphores[2*pid].acquire() # wait nonEmpty pid
    while [buffer[K*i] for i in range (NPROD)] != [-1]*NPROD: # hasta que los NPROD productores dejen de producir
        # Seleccionamos el menor dato del buffer y el productor correspondiente
        dato, pid = get_data(buffer, mutex)
        print (f"Consumimos {dato} del productor {pid}")
        # Traslado una posición a la izquierda los datos de dicho productor
        for i in range(K*pid, K*pid + ultimos[pid]):
            buffer[i] = buffer[i + 1]
        ultimos[pid] = ultimos[pid] - 1 
        # Lo añadimos a la lista resultado
        result.append(dato)
        lsemaphores[2*pid + 1].release() # signal nonFull pid
        lsemaphores[2*pid].acquire() # wait nonEmpty pid
        print("Buffer = " +str(list(buffer)))

def main():
    manager = Manager()
    result = manager.list() # Lista solución de números ordenados
    
    lsemaphores = []
    ultimos = Array('i', NPROD)    
    for i in range(NPROD):
        lsemaphores.append(Semaphore (0)) #non-Empty 
        lsemaphores.append(BoundedSemaphore (K)) #non-Full 
        ultimos[i] = -1
        
    buffer = Array ('i', K*NPROD)
    
    mutex = Lock()
    
    prodlst = [Process(target=producer, name=f'Productor {pid}', 
                       args=(pid, lsemaphores[2*pid], lsemaphores[2*pid + 1], buffer,
                             ultimos, mutex, randint(1,2*K))) for pid in range(NPROD) ]
    merge = Process (target = consumer, name ='Consumidor', 
                     args = (lsemaphores, buffer, ultimos, result, mutex))
    for p in prodlst + [merge]:
        p.start()

    for p in prodlst + [merge]:
        p.join()
        
    print(f"result = {result}")

if __name__ == '__main__':
    main()