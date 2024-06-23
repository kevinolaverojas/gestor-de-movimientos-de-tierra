import csv
import mysql.connector
from enum import Enum

# Definición del tipo de terreno con sus valores asociados
Tipo_terreno = Enum('Tipo_terreno', {
    'arcilla_natural': 1,
    'arcilla_seca': 2,
    'arcilla_humeda': 3,
    'grava_seca': 4,
    'grava_humeda': 5,
    'roca_75': 6,
    'roca_50': 7,
    'roca_25': 8
})

# Factores de corrección para cada tipo de terreno
factores_correccion = {
    Tipo_terreno.arcilla_humeda: 0.80,
    Tipo_terreno.arcilla_natural: 0.83,
    Tipo_terreno.arcilla_seca: 0.81,
    Tipo_terreno.roca_25: 0.80,
    Tipo_terreno.roca_50: 0.75,
    Tipo_terreno.roca_75: 0.70,
    Tipo_terreno.grava_humeda: 0.84,
    Tipo_terreno.grava_seca: 0.86
}

# Conectar a la base de datos
def conectar_db():
    """
    Establece una conexión a la base de datos MySQL.

    Esta función se conecta a una base de datos MySQL ubicada en localhost
    con el usuario 'root' y una contraseña vacía. La base de datos utilizada se llama 'movimientos'.

    Retorno:
    mysql.connector.connection.MySQLConnection: Objeto de conexión a la base de datos MySQL.
    
    Excepciones:
    mysql.connector.Error: Si ocurre un error al intentar conectarse a la base de datos,
    se lanzará una excepción especificando el tipo de error.
    """
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="movimientos")

# Agregar un movimiento de tierra a la base de datos
def agregar_movimiento(descriptor, volumen, tipo, coordenadas):
    """
    Agrega un nuevo movimiento de tierra a la base de datos MySQL.

    Esta función inserta registros en las tablas 'coordenadas', 'volumen', 'tipo', 'registro', 
    y 'movimientos' de la base de datos 'movimientos'. Utiliza una conexión establecida a través 
    de la función 'conectar_db'.

    Parámetros:
        descriptor (str): Descripción del movimiento de tierra.
        volumen (tuple): Tupla con las dimensiones del volumen (ancho, largo, alto) y el volumen total.
        tipo (Enum): Tipo de terreno, que incluy un valor y un factor de esponjamiento asociado.
        cordenadas (tuple): Tupla con las coordenadas UTM (norte, este).

    Excepciones:
        mysql.connector.Error: Si ocurre un error al ejecutar las operaciones SQL, se lanzará una excepción.

    
    """
    db = conectar_db()
    cursor = db.cursor()

    #try lo utilice por si  pueda ocurrir una excepción
    try:
        # Insertar coordenadas en la tabla 'coordenadas'
        cursor.execute("INSERT INTO coordenadas (norte, este) VALUES (%s, %s)", coordenadas)
        id_coordenadas = cursor.lastrowid

        # Insertar volumen en la tabla 'volumen'
        cursor.execute("INSERT INTO volumen (ancho, largo, alto, total) VALUES (%s, %s, %s, %s)", volumen)
        id_volumen = cursor.lastrowid

        # Insertar tipo en la tabla 'tipo'
        cursor.execute("INSERT INTO tipo (tipo, factor_de_esponjamiento) VALUES (%s, %s)", (tipo.value, factores_correccion[tipo]))
        id_tipo = cursor.lastrowid

        # Insertar registro en la tabla 'registro'
        cursor.execute("INSERT INTO registro (fecha_de_registro, opcion) VALUES (CURDATE(), %s)", (opcion,))
        id_registro = cursor.lastrowid

        # Insertar movimiento en la tabla 'movimientos'
        cursor.execute("INSERT INTO movimientos (descriptor, id_volumen, id_tipo, id_coordenadas, id_registro) VALUES (%s, %s, %s, %s, %s)", 
                       (descriptor, id_volumen, id_tipo, id_coordenadas, id_registro))
        db.commit()
    # garantiza que se cierre bien la base de datos, incluso si ocuure un error 
    finally:
        cursor.close()
        db.close()

# Obtener todos los movimientos de tierra desde la base de datos
def obtener_movimientos():
    """
    Obtiene todos los movimientos de tierra desde la base de datos MySQL.

    Esta función consulta la base de datos 'movimientos' y recupera una lista de todos los movimientos de tierra.
    La información recuperada incluye el descriptor del movimiento, el volumen total, el tipo de terreno, 
    y las coordenadas UTM (este y norte).

    Retorno:
        Una lista de tuplas donde cada tupla contiene la siguiente información:
            - descriptor (str): Descripción del movimiento de tierra.
            - total (float): Volumen total del movimiento.
            - id_tipo (int): ID del tipo de terreno.
            - este (float): Coordenada UTM este.
            - norte (float): Coordenada UTM norte.
    
    """
    db = conectar_db()
    cursor = db.cursor()
    # consulta sql para obtener lo antes mencionado
    cursor.execute(
        "SELECT m.descriptor, v.total, t.id_tipo, c.este, c.norte "
        "FROM movimientos m "
        "JOIN volumen v ON m.id_volumen = v.id_volumen "
        "JOIN tipo t ON m.id_tipo = t.id_tipo "
        "JOIN coordenadas c ON m.id_coordenadas = c.id_coordenadas"
    )
    movimientos = cursor.fetchall()

    cursor.close()
    db.close()
    return movimientos

# Calcular la cubicación total de los movimientos de tierra
def calcular_cubicacion_total():
    """
    Calcula la cubicación total de todos los movimientos de tierra.

    Esta función obtiene todos los movimientos de tierra desde la base de datos utilizando la función 
    `obtener_movimientos`. Luego, calcula la cubicación total, considerando el volumen y el factor de 
    corrección asociado al tipo de terreno.

    Retorno:
        La cubicación total de todos los movimientos de tierra, ajustada por los factores de corrección.

    """
    movimientos = obtener_movimientos()
    cubicacion_total = 0

    for movimiento in movimientos:
        volumen = movimiento[1]
        tipo = Tipo_terreno(movimiento[2])
        factor = factores_correccion[tipo]
        cubicacion_total += volumen * factor + volumen

    return cubicacion_total
# Función para generar el informe de los movimientos de tierra

def generar_informe_csv(ruta_archivo):
    """
    Genera un informe detallado de las cubicaciones de todos los movimientos de tierra registrados en formato CSV.

    Esta función obtiene todos los movimientos de tierra desde la base de datos utilizando la función `obtener_movimientos`.
    Luego, genera un informe que incluye el descriptor del movimiento, el volumen, el tipo de terreno, las coordenadas UTM
    y el volumen ajustado por esponjamiento para cada movimiento. Al final, el informe incluye la cubicación total y se guarda
    en un archivo CSV en la ruta especificada.

    Parámetros:
        ruta_archivo (str): La ruta del archivo CSV donde se guardará el informe.

    """
    movimientos = obtener_movimientos()
    if not movimientos:
        print("No hay movimientos de tierra registrados. No se puede generar el informe.")
        return
    
    with open(ruta_archivo, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(["Descriptor", "Volumen (m³)", "Tipo de Terreno", "Coordenadas UTM (Este, Norte)", "Volumen con Esponjamiento (m³)"])

        for movimiento in movimientos:
            descriptor = movimiento[0]
            volumen = movimiento[1]
            tipo = Tipo_terreno(movimiento[2])
            coordenadas = (movimiento[3], movimiento[4])
            volumen_esponjamiento = volumen * (1 + factores_correccion[tipo])
            writer.writerow([descriptor, volumen, tipo.name, f"{coordenadas[0]}, {coordenadas[1]}", volumen_esponjamiento])
        
        writer.writerow([])
        writer.writerow(["Cubicación total", calcular_cubicacion_total()])
    
    print(f"Informe generado y guardado en {ruta_archivo}")

# Cargar movimientos desde un archivo CSV y los agrega a la base de datos
def cargar_movimientos(filename):
    """
    Carga movimientos de tierra desde un archivo CSV y los agrega a la base de datos.

    Esta función lee un archivo CSV y agrega cada movimiento de tierra a la base de datos. 
    Cada fila del archivo debe contener la siguiente información: descriptor, ancho, largo, 
    altura, ID del tipo de terreno, coordenada UTM este y coordenada UTM norte. El volumen 
    se calcula multiplicando ancho, largo y altura.

    Parámetros:
        filename (str): Ruta del archivo CSV que contiene los movimientos de tierra.

    Excepciones:
        FileNotFoundError: Si el archivo especificado no se encuentra.
        Exception: Si ocurre cualquier otro error durante la carga de movimientos, se imprime 
                   un mensaje de error con la descripción del problema.

    """
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file, delimiter=';')
            header = next(reader)  # Saltar la cabecera
            print(f"Cabecera: {header}")
            for row in reader:
                if len(row) < 7:
                    print(f"Fila incompleta: {row}")
                    continue
                descriptor = row[0]
                ancho = float(row[1])
                largo = float(row[2])
                altura = float(row[3])
                volumen = (ancho, largo, altura, ancho * largo * altura)
                tipo = Tipo_terreno(int(row[4]))
                coordenadas = (row[5], row[6])
                agregar_movimiento(descriptor, volumen, tipo, coordenadas)
        print(f"Movimientos cargados desde {filename}")
    except FileNotFoundError:
        print(f"Error: El archivo '{filename}' no fue encontrado.")
    except Exception as e:
        print(f"Error al cargar los movimientos: {e}")
        
# Editar un movimiento de tierra existente
def editar_movimiento():
    """
    Permite editar un movimiento de tierras existente en la base de datos.

    El usuario debe ingresar el descriptor del movimiento que desea editar. Luego, se solicitan los nuevos valores
    para la altura, largo, ancho, tipo de terreno y coordenadas UTM. Estos valores se utilizan para actualizar los
    registros en las tablas 'volumen', 'tipo' y 'coordenadas' correspondientes al movimiento de tierras especificado.


    Excepciones:
        - ValueError: Si se ingresan valores no válidos para la altura, largo, ancho, tipo de terreno o coordenadas UTM.
        - Exception: Cualquier otro error que pueda ocurrir durante el proceso de edición.

    """
    
    descriptor = input("Ingrese el descriptor del movimiento que desea editar: ")
    db = conectar_db()
    cursor = db.cursor()

    cursor.execute("SELECT id_volumen, id_tipo, id_coordenadas FROM movimientos WHERE descriptor = %s", (descriptor,))
    resultado = cursor.fetchone()
    if not resultado:
        print(f"No se encontró un movimiento con el descriptor {descriptor}.\n")
        return

    id_volumen, id_tipo, id_coordenadas = resultado

    try:
        while True:
            try:
                altura = float(input("Ingrese la nueva altura del movimiento de tierras en metros: "))
                largo = float(input("Ingrese el nuevo largo del movimiento de tierras en metros: "))
                ancho = float(input("Ingrese el nuevo ancho del movimiento de tierras en metros: "))
                if altura <= 0 or largo <= 0 or ancho <= 0:
                    raise ValueError("Todas las dimensiones deben ser números positivos, vuelva a ingresar.")
                volumen = (ancho, largo, altura, ancho * largo * altura)
                break
            except ValueError as e:
                print(e)
        
        while True:
            try:
                tipo_valor = int(input("Seleccione el nuevo tipo de terreno 'arcilla natural = 1, arcilla seca = 2, arcilla humeda = 3, grava seca = 4, grava humeda = 5, roca 75% = 6, roca 50% = 7, roca 25% = 8 ': "))
                tipo = Tipo_terreno(tipo_valor)
                break
            except ValueError:
                print("Tipo de terreno no válido. Seleccione un tipo de terreno válido.")
        
        while True:
            try:
                este = float(input("Ingrese la coordenada UTM Este: "))
                norte = float(input("Ingrese la coordenada UTM Norte: "))
                if not (0 <= este <= 10000) or not (0 <= norte <= 10000):
                    raise ValueError("Las coordenadas deben estar en el rango de 0 a 10000.")
        
                coordenadas = (este, norte)
                agregar_movimiento(numero, volumen, tipo, coordenadas)
                break
            except ValueError:
                print(f"tipo de coordenada invalida ingrese nuevamente")
                
         
        cursor.execute("UPDATE volumen SET ancho = %s, largo = %s, alto = %s, total = %s WHERE id_volumen = %s", (*volumen, id_volumen))
        cursor.execute("UPDATE tipo SET tipo = %s, factor_de_esponjamiento = %s WHERE id_tipo = %s", (tipo.value, factores_correccion[tipo], id_tipo))
        cursor.execute("UPDATE coordenadas SET norte = %s, este = %s WHERE id_coordenadas = %s", (*coordenadas, id_coordenadas))
        cursor.execute("INSERT INTO registro (fecha_de_registro, opcion) VALUES (CURDATE(), %s)", ("Editar",))
        db.commit()
        print(f"Movimiento de tierras {descriptor} editado correctamente.\n")
    except Exception as e:
        print(f"Error al editar el movimiento: {e}")
    finally:
        cursor.close()
        db.close()

# Eliminar un movimiento de tierra existente
def eliminar_movimiento():
    """
    Elimina un movimiento de tierras existente en la base de datos.

    El usuario debe ingresar el descriptor del movimiento que desea eliminar. Luego, se eliminan los registros 
    correspondientes al movimiento en las tablas 'movimientos', 'volumen', 'tipo' y 'coordenadas'.

    Excepciones:
        - Exception: Se lanza si ocurre un error durante el proceso de eliminación.

    """
    descriptor = input("Ingrese el descriptor del movimiento que desea eliminar: ")
    db = conectar_db()
    cursor = db.cursor()

    cursor.execute("SELECT id_volumen, id_tipo, id_coordenadas FROM movimientos WHERE descriptor = %s", (descriptor,))
    resultado = cursor.fetchone()
    if not resultado:
        print(f"No se encontró un movimiento con el descriptor {descriptor}.\n")
        return

    id_volumen, id_tipo, id_coordenadas = resultado

    try:
        cursor.execute("DELETE FROM movimientos WHERE descriptor = %s", (descriptor,))
        cursor.execute("DELETE FROM volumen WHERE id_volumen = %s", (id_volumen,))
        cursor.execute("DELETE FROM tipo WHERE id_tipo = %s", (id_tipo,))
        cursor.execute("DELETE FROM coordenadas WHERE id_coordenadas = %s", (id_coordenadas,))
        cursor.execute("INSERT INTO registro (fecha_de_registro, opcion) VALUES (CURDATE(), %s)", ("Eliminar",))
        db.commit()
        print(f"Movimiento de tierras {descriptor} eliminado correctamente.\n")
    except Exception as e:
        print(f"Error al eliminar el movimiento: {e}")
    finally:
        cursor.close()
        db.close()

# Función para mostrar el menú de opciones
def mostrar_menu():
    """
    Muestra un menú de opciones y solicita al usuario que seleccione una.

    El menú incluye las siguientes opciones:
    1. Agregar movimiento de tierra
    2. Calcular volumen total de esponjamiento
    3. Generar informe por consola
    4. Cargar movimientos de tierras desde un archivo csv
    5. Editar movimiento de tierra
    6. Eliminar movimiento de tierra
    7. Salir

    La función solicita al usuario que seleccione una opción ingresando el número correspondiente.

    Retorna:
        str: La opción seleccionada por el usuario.

    """
    
    print("1. Agregar movimiento de tierra")
    print("2. Calcular volumen total de esponjamiento")
    print("3. Generar informe por consola")
    print("4. Cargar movimientos de tierras desde un archivo csv")
    print("5. Editar movimiento de tierra")
    print("6. Eliminar movimiento de tierra")
    print("7. Salir")
    opcion = input("Seleccione una opción: ")
    return opcion


# Ciclo principal del programa
while True:
    opcion = mostrar_menu()

    if opcion == "1":
        numero = input("Asigne un descriptor al movimiento de tierras: ")
        
        if any(movimiento[0] == numero for movimiento in obtener_movimientos()):
            print(f"El descriptor '{numero}' ya existe. Por favor, elija otro descriptor.")
            continue
        
        while True:
            try:
                altura = float(input("Ingrese la altura del movimiento de tierras en metros: "))
                largo = float(input("Ingrese el largo del movimiento de tierras en metros: "))
                ancho = float(input("Ingrese el ancho del movimiento de tierras en metros: "))
                
                if altura <= 0 or largo <= 0 or ancho <= 0:
                    raise ValueError("Todas las dimensiones deben ser números positivos, vuelva a ingresar.")
                volumen = (ancho, largo, altura, altura * largo * ancho)
                break
            
            except ValueError as e:
                print(e)
        
        while True:
            try:
                tipo_valor = int(input("Seleccione el tipo de terreno 'arcilla natural = 1, arcilla seca = 2, arcilla humeda = 3, grava seca = 4, grava humeda = 5, roca 75% = 6, roca 50% = 7, roca 25% = 8 ': "))
                tipo = Tipo_terreno(tipo_valor)
                break
            except ValueError:
                print("Tipo de terreno no válido. Seleccione un tipo de terreno válido.")
        
        while True:
            try:
                este = float(input("Ingrese la coordenada UTM Este: "))
                norte = float(input("Ingrese la coordenada UTM Norte: "))
                if not (0 <= este <= 10000) or not (0 <= norte <= 10000):
                    raise ValueError("Las coordenadas deben estar en el rango de 0 a 10000.")
        
                coordenadas = (este, norte)
                agregar_movimiento(numero, volumen, tipo, coordenadas)
                break
            except ValueError:
                print(f"tipo de coordenada invalida ingrese nuevamente")
                
        print(f"Movimiento de tierras {numero} agregado correctamente.\n")      
        
        

    elif opcion == "2":
        cubicacion_total = calcular_cubicacion_total()
        print(f"\nVolumen total: {cubicacion_total:.2f} m³\n")

    elif opcion == "3":
        ruta_archivo = input("Ingrese la ruta del archivo CSV para guardar el informe: ")
        generar_informe_csv(ruta_archivo)

    elif opcion == "4":
        filename = input("Ingrese la ruta del archivo para cargar: ")
        cargar_movimientos(filename)

    elif opcion == "5":
        editar_movimiento()

    elif opcion == "6":
        eliminar_movimiento()

    elif opcion == "7":
        print("Saliendo del programa.")
        break