import csv 
import pymongo
import time
import random
import datetime

#Commando para instalar la libreria; python3 -m pip install pymongo

#Este script es el script con generación de datos
#Es decir teniendo en cuenta:
#las visitas/consultas,fechas de las visitas, los pesos y las tallas.

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
myclient.drop_database('practica_4')
file = open('/home/lab/DABD/practica_4/Breeds.csv')
breeds = csv.reader(file, delimiter=';')
header = []
header = next(breeds)

#-------------------Extensión datos Razas---------------------

#Funcion que genera una talla o bien en función de dos números min y max o bien
#en función del tamaño
def generate_talla(num1,num2,dimension):
    if (num1 != '' and num1 != 'na' and num1 != 'not found') and (num2 != '' and num2 != "na" and num1 != 'not found'):
        if '.' in num1 or '.' in num2:
            return random.randint(int(float(num1)),int(float(num2)))
        else:
            return random.randint(int(num1),int(num2))
    elif dimension != "" and (num1 == "" and num2 == ""): 
        if dimension == "X-Small":
            return random.randint(7,12)
        if dimension == "Small":
            return random.randint(10,20)
        if dimension == "Medium":
          return random.randint(15,25)
        if dimension == "Large":
          return random.randint(20,30)
        if dimension == "X-Large":
          return random.randint(23,33)
    else:
        return random.randint(5,30)
    
#misma funcion que talla pero adaptada a los pesos. 
def generate_peso(num1,num2,dimension):

    if (num1 != '' and num1 != 'na' and num1 != 'not found') and (num2 != '' and num2 != "na" and num1 != 'not found'):
        if '.' in num1 or '.' in num2:
            return random.randint(int(float(num1)),int(float(num2)))
        else:
            return random.randint(int(num1),int(num2))
    elif dimension != "" and (num1 == "" and num2 == ""): 
        if dimension == "X-Small":
            return random.randint(8,17)
        if dimension == "Small":
            return random.randint(20,35)
        if dimension == "Medium":
          return random.randint(35,50)
        if dimension == "Large":
          return random.randint(50,60)
        if dimension == "X-Large":
          return random.randint(65,100)
    else:
        return random.randint(20,70)


lista_pesos=[]
lista_tallas=[]
for row in breeds:   
    if(row[1] != "height_low_inches"):
       #Para generar la talla y el peso, coge el maximo y minimo respectivo
       #pero tambien coje el tamaño, si no hay númericos se estima un número en función
       #del tamaño de cada animal
       talla = generate_talla(row[1],row[2],row[5])
       lista_tallas.append(talla)
       peso = generate_peso(row[3],row[4],row[5])
       lista_pesos.append(peso)

file.close()
file = open('/home/lab/DABD/practica_4/Breeds.csv')
breeds = csv.reader(file, delimiter=';')
#print(len(lista_tallas)) #perfecto  


massive_dict = dict.fromkeys(header)
breeds_list = []
talla_index = 0
peso_index = 0
#Metemos en el mismo diccionario los pesos y tallas
for row in breeds:
    if(row[1] != "height_low_inches"):
        #print("r",row[0])
        new_dict = dict(zip(header, row))
        new_dict["Talla"] = lista_tallas[talla_index]
        new_dict["Peso"] = lista_pesos[peso_index]
        #print("p",talla_index)
        breeds_list.append(new_dict)
        talla_index+=1
        peso_index+=1
            
#----------------------------------Razas-Hechas----------------------------------------------


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["practica_4"]
mycol = mydb["razas"]
#coleccion razas
for doc in breeds_list:
    
    raza = {
        'id':doc['\ufeffID'],
        'nombre':doc['R2'],
        'comportamiento':doc['Info.temperament'],
        'entrenamiento':doc['Info.trainability_category'],
        'caracteristicas':doc['Info.description'],
    }
    
    x = mycol.insert_one(raza)
t1 = time.time()
print("Colección 'razas' subida correctamente en; ", t1)


file.close()
#---------------------------------Veterinarios--Hechos----------------------------------------
file = open('/home/lab/DABD/practica_4/Clinics_and_vets.csv')
vets = csv.reader(file, delimiter=';')

header = []
header = next(vets)

massive_dict = dict.fromkeys(header)
vets_list = []

for row in vets:
    vets_list.append(dict(zip(header,row)))
   
#coleccion veterinarios
col_vets = mydb["veterinarios"]
index_id_users = 0
for doc in vets_list:
    
    vets = {
	"id_veterinario":doc['\ufeffvet_id'],
	"registration_Date":doc['vet.registration_date'],
		"Usuario":{
		"id_usuario": index_id_users,
		"username" : doc['vet.login']
     }
    }

    index_id_users = index_id_users + 1
    x = col_vets.insert_one(vets)
t2 = time.time()
print("Colección 'veterinarios' subida correctamente en: ", t2)

#---------------------------------Clinicas--Hechos----------------------------------------
clinics_id_list = []
for a in vets_list:
    x = []
    x.append(a['clinic_id'])
    x.append(a['Clinics.email'])
    x.append(a['Clinics.name'])
    if x not in clinics_id_list:
        clinics_id_list.append(x)

col_clinicas = mydb["clinicas"]

for doc in clinics_id_list:
    vets_in_clinic = []
    for i in vets_list:
        if doc[0] == i['clinic_id']:
            vets_in_clinic.append(i['\ufeffvet_id'])
            
    clinics = {
	"clinica_id":doc[0],
    "clinica_nombre":doc[2],
	"email":doc[2],
    "veterinarios":vets_in_clinic
    }

    x = col_clinicas.insert_one(clinics)


t3 = time.time()
print("Colección 'clinicas' subida correctamente en; ", t3)


#---------------------------------Clientes-Hecho-----------------------------------------

file = open('/home/lab/DABD/practica_4/Pets_and_clients.csv')
pets_clients = csv.reader(file, delimiter=';')

header = []
header = next(pets_clients)

massive_dict = dict.fromkeys(header)
client_list = []
for row in pets_clients:
    client_list.append(dict(zip(header,row)))

client_ids = []
mascota_id= []
clients_pets_id =[]
index = 0
for a in client_list:
    if client_ids == []:
        client_ids.append([a['Client_id'],a['Client.login']])
   
    checkit = [a['Client_id'],a['Client.login']]
    if checkit not in client_ids and checkit != ['', ''] :
        client_ids.append([a['Client_id'],a['Client.login']])
        
    if a not in clients_pets_id:
        clients_pets_id.append([a['Client_id'],a['Pet_ID']])


#Funcion que busca los ids de los animales por el id del cliente, es decir busca el dueño del animal
def find_owner_pets(array,id):
    list_animales=[]
    for a in array:
        if a[0] != '':
            if int(float(a[0])) == id:
                list_animales.append(a[1])
    return list_animales


#coleccion clientes
col_clientes = mydb["clientes"]
for a in client_ids:
    if '.' in a[0]:
        id = int(float(a[0]))
    else:
        id = int (a[0])  
    clientes =   {
       "id_cliente" : id,
       "mascotas" : find_owner_pets(clients_pets_id,id), 
       "Usuario":{
	       "id_usuario": index_id_users, 
	       "User_Login" : a[1]	       
                 }
             }           
    index_id_users = index_id_users + 1
    x = col_clientes.insert_one(clientes)

t4 = time.time()
print("Colección 'clientes' subida correctamente en; ", t4)
t4 = time.time()
print("Colección 'clientes' subida correctamente en; ", t4)

    
#--------------------------------Mascotas--Falta Extensión----------------------------------------
id_raza_list=[]
for doc in breeds_list:
    if doc['R2'] not in id_raza_list:
        id_raza_list.append([[doc['\ufeffID']],[doc['R2']]])

#funcion que busca el id de la raza en funcion del nombre de la raza de la mascota
def find_id_with_raza(raza,list):
    found = False
    for a in list:
        if str(a[1][0]) == raza:
            id = a[0][0]
            found = True
    if(found == True):
        return id        
    if(found == False):
        #print("a") Por lo menos en la mitad de las razas no las encuentra en Breed. 
        return ""
        
#-------------------------------Consultas----------------------------------
#funcion que genera una mascota aleatoria de entre las posibles devolviendo su id.
def random_pet():
    index_pet = random.randint(0,len(client_list)-1)
    return client_list[index_pet]["Pet_ID"]

#funcion que genera un veterinario aleatori de entre las posibles devolviendo su id.
def random_vet():
    index_vet = random.randint(0,len(vets_list)-1)
    return vets_list[index_vet]["\ufeffvet_id"]

#Esta funcion devuelve una fecha para cada animal, se tiene en cuenta la fecha de registro
#de cada mascota para generar una posterior, si no tiene  fecha de registro, asignamos una fecha por defecto. 
def get_date(pet_id):
    for pet in client_list:
        if pet["Pet_ID"] == pet_id:
            reg_date_raw = pet["RegistrationDate"]
            reg_date = reg_date_raw.split()

            if not reg_date:
                return "2020-01-01"
            else:
                reg_date_clean = reg_date[0].split("/")
                if int(reg_date_clean[1]) < 10:
                    day = "0"+reg_date_clean[1]
                else:
                    day = reg_date_clean[1]
                return reg_date_clean[2]+"-"+reg_date_clean[0]+"-"+day

    return "2020-01-01"


#funcion que obtiene el peso de el diccionario de razas, gracias al id de la mascota
def get_peso(pet_id):
    for pet in client_list:
        if pet["Pet_ID"] == pet_id:
            for breed in breeds_list:
                if breed["R2"] == pet["\ufeffBreed"]:
                   return int(breed['Peso'])
    #En caso de no encontrar la raza del animal en el dataset de razas, 
    #se genera un peso aleatorio entre 20 y 80. 
    return random.randint(20,80)

#funcion que obtiene el peso del diccionario de razas, gracias al id de la mascota
def get_talla(pet_id):
    
    for pet in client_list:
        if pet["Pet_ID"] == pet_id:
            for breed in breeds_list:
                
                if breed["R2"] == pet["\ufeffBreed"]:
                    return int(breed['Talla'])
    #En caso de no encontrar la raza del animal en el dataset de razas, 
    #se genera una talla aleatorio entre 20 y 80. 
    return random.randint(10,30) 


#con la start_date siendo la fecha de registro de cada animal y la end_date la fecha obtenida 
#por la funcion get_date, generamos una fecha aleatoria entre estas dos fechas. 
def generate_random_date(start_date, end_date):
 
    start_timestamp = datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp()
    end_timestamp = datetime.datetime.strptime(end_date, '%Y-%m-%d').timestamp()
    random_timestamp = random.uniform(start_timestamp, end_timestamp)
    random_date = datetime.datetime.fromtimestamp(random_timestamp).strftime('%Y-%m-%d')
    
    return random_date


col_consultas = mydb["consultas"]
id_rev= 0
list_id_consultas = []
consultas_dict = {}
consultas_of_mascota = []

for i in range(0,len(client_list)):
    list_id_consultas.append(i)

#coleccion de consultas
for a in list_id_consultas:
    mascota = random_pet()
    vet = random_vet()
    if mascota not in consultas_dict:
        list_consulta = [a]
        consultas_dict[mascota] = list_consulta
    else:
        consultas_dict[mascota].append(a)
    consulta = {
        "id_consulta":a, 
        #Esto hay que ver si hacerlo mas currado porque igual la fecha de consulta de la mascota
        #tiene que ser posterior a su fecha de registro, tiene sentido. 
	    "fecha_consulta":generate_random_date(get_date(mascota) ,'2023-04-29'), #pendiente
        "mascota":mascota,
	    "veterinario":vet,	
	    "revision": {
            "id_revision": id_rev,
            "peso": get_peso(mascota), 
            "talla": get_talla(mascota) 
             }
        }
    x =col_consultas.insert_one(consulta)    
    id_rev+=1

t5 = time.time()
print("Colección 'consulta' subida correctamente en; ", t5)


#coleccion de mascotas
col_mascotas = mydb["mascotas"]
consultas = {}
for doc in client_list:
    if doc['Pet_ID'] in consultas_dict:
        consultas = consultas_dict[doc['Pet_ID']]
    if doc['Client_id'] == '':
        id_client = ""
    elif doc['Client_id'] != '':
        id_client = int(float(doc['Client_id']))
    
    mascota = {
        "id_mascota": doc['Pet_ID'],
        "id_cliente":str(id_client), 
	    "nombre": doc['DogName'],
        "Color": doc['Color'],
	    "register_date" : doc['RegistrationDate'],
	    "Raza":find_id_with_raza(doc['\ufeffBreed'],id_raza_list), #Si no existe el nombre de raza en el dataset, se mete vacio. 
	    "consultas": consultas
        
    }
    #print("consultas: ", consultas)
    x =col_mascotas.insert_one(mascota)

t6 = time.time()
print("Colección 'mascotas' subida correctamente en; ", t6)

t7 = time.time()

print("Ejecución terminada en: ", t7)