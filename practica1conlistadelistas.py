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

- OPCIONAL: mente se puede hacer un búffer de tamaño fijo de forma que
los productores ponen valores en el búffer.
"""
"""
VERSIÓN 3: Buffer en forma de lista de listas 
"""
# Importamos los paquetes necesarios
from multiprocessing import Process, Manager, BoundedSemaphore, Semaphore, Lock
from random import randint

#Fijamos el número de productores
NPROD = 3
#Fijamos capacidad máxima de elementos almacenados por productor
K = 10

"""
La función add_data añade a la posición correspondiente del buffer el dato 
producido por el productor pid. Se protege con un Lock (mutex) para que 
productor y consumidor no puedan producir y consumir a la vez.
"""        
def add_data(lbuffers, pid, dato, mutex):
    mutex.acquire()
    try:
        lista = lbuffers[pid]
        lista.append(dato)
        lbuffers[pid] =lista
    finally:
        mutex.release()

"""
La función get_data obtiene el elemento mínimo del buffer y el productor que
lo produjo. Se protege con un Lock (mutex) para que productor y consumidor 
no puedan producir y consumir a la vez.
"""
def get_data(lbuffers, mutex):
    mutex.acquire()
    try:
        buffer = [lbuffers[i][0] for i in range(NPROD)]
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
def producer(pid, nonEmpty, nonFull, lbuffers, mutex, numero):
    v = 0
    for dato in range(numero):
        v += randint(1,6)
        nonFull.acquire() # wait nonFull
        # Se añade el dato generado al buffer
        add_data(lbuffers,pid, v, mutex)
        print (f"Productor {pid} almacenando {v}")
        nonEmpty.release() # signal nonEmpty
    # El productor produce un -1 para indicar que ha terminado de producir
    nonFull.acquire() # wait nonFull
    add_data(lbuffers, pid, -1, mutex)# Añadimos el -1 al buffer
    nonEmpty.release() # wait nonEmpty

"""
La función consumer corresponde a la acción de consumir el menor elemento del
buffer.
"""
def consumer(lsemaphores, lbuffers, result, mutex):
    # Primeramente, nos aseguramos de que todos los productores hayan producido
    for pid in range(NPROD):
        if list(lbuffers)[pid] == []:
            lsemaphores[2*pid].acquire() # wait nonEmpty pid
    while list(lbuffers) != [[-1]]*NPROD: # hasta que los NPROD productores dejen de producir
        # Seleccionamos el menor dato del buffer y el productor correspondiente      
        dato, pid = get_data(lbuffers, mutex)
        print (f"Consumimos {dato} del productor {pid}")
        lbuffers[pid] = lbuffers[pid][1:]
        # Lo añadimos a la lista resultado
        result.append(dato)
        lsemaphores[2*pid + 1].release() # signal nonFull pid
        lsemaphores[2*pid].acquire() # wait nonEmpty pid
        print("Buffer = " +str(list(lbuffers)))

def main():
    manager = Manager()
    result = manager.list()
    lbuffers = manager.list()
    
    lsemaphores=[]
    for i in range(NPROD):
        lsemaphores.append(Semaphore (0)) #non-Empty (para q el consumidor antes de consumir vea que puede)
        lsemaphores.append(BoundedSemaphore (K)) #non-Full (para que el productor antes de producir vea q no excede la cap máxima 1)
        lbuffers.append([])

    mutex = Lock()
    
    prodlst = [Process(target=producer, name=f'Productor {pid}', 
                       args=(pid, lsemaphores[2*pid], lsemaphores[2*pid + 1], lbuffers,
                             mutex, randint(1,2*K))) for pid in range(NPROD) ]
    merge = Process (target = consumer, name ='Consumidor', 
                     args = (lsemaphores, lbuffers, result, mutex))
    
    for p in prodlst + [merge]:
        p.start()

    for p in prodlst + [merge]:
        p.join()
        
    print(f"result = {result}")

if __name__ == '__main__':
    main()