

"""
Taller evaluable: ejercicio de un Hadoop MapReduce para contar palabras
Este script simula el proceso MapReduce para contar palabras en archivos de texto.
"""


# Importación de módulos necesarios
import glob  # Para buscar archivos usando patrones
import os    # Para operaciones de sistema de archivos
import string # Para manipulación de cadenas y puntuación
import time   # Para medir el tiempo de ejecución


# Definición de funciones

def clear_input_directory(input_dir):
    """
    Limpia la carpeta de entrada eliminando todos los archivos existentes.
    Si la carpeta no existe, la crea.
    """
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    else:
        for file in glob.glob(f"{input_dir}/*"):
            os.remove(file)
    
def generate_file_copies(n, input_dir, raw_dir):
    """
    Genera n copias de cada archivo en la carpeta raw_dir y las guarda en input_dir.
    """
    for file in glob.glob(f"{raw_dir}/*"):
        with open(file, "r", encoding="utf-8") as f:
            text = f.read()

        for i in range(n):
            raw_filename_with_extension = os.path.basename(file)
            raw_filename_without_extension = os.path.splitext(raw_filename_with_extension)[0]
            new_filename = f"{raw_filename_without_extension}_{i}.txt"

            with open(f"{input_dir}/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)

def mapper(sequence):
    """
    Función Mapper: convierte cada línea en una secuencia de pares (palabra, 1).
    Elimina puntuación y convierte a minúsculas.
    """
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])
    return pairs_sequence

def reducer(pairs_sequence):
    """
    Función Reducer: suma los valores de cada palabra para obtener el conteo total.
    Requiere que la secuencia esté ordenada por palabra.
    """
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result

def hadoop(input_dir, output_dir, mapper_func, reducer_func):
    """
    Simula el proceso Hadoop MapReduce:
    1. Lee líneas de archivos de entrada.
    2. Aplica el mapper.
    3. Ordena los pares (shuffle and sort).
    4. Aplica el reducer.
    5. Escribe el resultado y crea el archivo de éxito.
    """

    # Lee los archivos de files/input y devuelve una lista de tuplas (archivo, línea)
    def extract_lines_from_files(input_dir):
        sequence = []
        files = glob.glob(f"{input_dir}/*")
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence
    
    # Ordena la secuencia de pares por la palabra (shuffle and sort)
    def suffler_and_sort(pairs_sequence):
        pairs_sequence = sorted(pairs_sequence)
        return pairs_sequence
    
    # Crea la carpeta files/output si no existe, si existe lanza error
    def create_output_folder(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        else:
            # Si ya existe, elimina todos los archivos dentro
            for file in glob.glob(f"{output_dir}/*"):
                os.remove(file)

    # Guarda el resultado en un archivo files/output/part-00000
    def write_word_count_to_file(result, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    # Crea el archivo _SUCCESS en files/output
    def create_success_file(output_dir):
        with open(f"{output_dir}/_SUCCESS", "w", encoding="utf-8") as f:
            f.write("")
    
    # Ejecución de las etapas MapReduce
    sequence = extract_lines_from_files(input_dir)
    pairs_sequence = mapper_func(sequence)
    pairs_sequence = suffler_and_sort(pairs_sequence)
    result = reducer_func(pairs_sequence)
    create_output_folder(output_dir)
    write_word_count_to_file(result, output_dir)
    create_success_file(output_dir)

# Ejecuta el experimento completo de MapReduce
def run_experiment(n):
    """
    Ejecuta el experimento MapReduce:
    1. Limpia la carpeta de entrada.
    2. Genera copias de archivos.
    3. Ejecuta el proceso MapReduce.
    4. Mide y muestra el tiempo de ejecución.
    """
    input_dir = "files/input"
    output_dir = "files/output"
    raw_dir = "files/raw"
    clear_input_directory(input_dir)
    generate_file_copies(n, input_dir, raw_dir)
    start_time = time.time() 
    hadoop(input_dir, output_dir, mapper, reducer)
    end_time = time.time() 
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")

if __name__ == "__main__":
    # Ejecuta el experimento con 500 copias por archivo
    run_experiment(500)